---
name: analyse-cohorte
description: Analyse la performance des cohortes d'accompagnement 1001 mots — taux d'appels OK et de petites missions par session, comparaison projeté/réalisé à partir de l'historique glissant des accompagnantes, décomposition des écarts par accompagnante, et diagnostic des facteurs explicatifs. À utiliser dès qu'une question porte sur la performance d'une cohorte, d'une session d'appel ou d'une accompagnante, sur une baisse ou une hausse constatée des appels OK ou des missions posées, ou sur la comparaison entre cohortes.
---

# Analyse de cohorte

## Comment exécuter les requêtes

Metabase n'est pas interrogeable depuis Claude Code sur ce projet. Passer par dbt,
qui utilise le profil Snowflake du repo :

```bash
cd dbt && dbt show --profiles-dir . --inline "$(cat <fichier>.sql)" --limit 50
```

`dbt show` tronque l'affichage à ~6 colonnes et coupe les valeurs texte longues. Pour
une requête large, la lancer en deux passes avec des sous-ensembles de colonnes plutôt
que d'essayer de tout afficher d'un coup.

Base : `MOTS_DW`, schémas `fact` / `dim`. Les requêtes de `sql/` sont collables telles
quelles dans une question native Metabase.

## Les quatre règles non négociables

Ces choix ont été validés sur données réelles. Les changer sans raison produit des
résultats faux.

1. **Dénominateur = appels ayant un statut renseigné** (`a_statut = 1`). Environ 12 %
   des lignes n'ont pas de statut, et ce sont presque exclusivement des parcours
   arrêtés ou désengagés (0 % de statut manquant chez les familles `active`, 70 % chez
   les `stopped`). Compter toutes les familles fabrique une fausse érosion session
   après session : 63 % → 44 % au lieu de 66 % → 64 %. Mesurer la couverture
   (`nb_traites / nb_familles`) comme indicateur **séparé** — c'est elle qui porte la
   perte réelle, elle tombe à 63-72 % en session 3.

2. **Une ligne par famille × session**, pas par enfant. Les fratries partagent le même
   appel. Agréger par `family_id` avec `max()` avant tout calcul.

3. **Ne garder que les sessions terminées** (`session_end <= current_date`). Sans ce
   filtre les cohortes en cours apparaissent à ~0 %.

4. **La projection exclut la cohorte évaluée** (`h.cohort_name <> v.cohort_name`).
   Sinon la cohorte entre dans sa propre projection et tous les écarts s'écrasent vers
   zéro.

## Définitions

| Notion | Colonne | Règle |
|---|---|---|
| Appel OK | `callN_status = 'OK'` | **Pas** `is_callN_status`, qui vaut 1 dès que le statut est renseigné quel qu'il soit |
| Petite mission (PM) | `is_callN_goals` | Vaut 1 si `callN_goals_sms` est non vide. Dénominateur = appels OK, qui ont toujours un statut — cette métrique n'est donc pas affectée par la règle 1 |
| Cohorte | `cohort_name` | Format `YYYY/MM/DD - Libellé`. Les cohortes 2026 = `cohort_name like '2026/%'` |
| Session | 0 à 3 | Fenêtre d'appel de 13 jours, bornée par `dim.groups.date_callN_start/end` |
| Accompagnante | `supporter_name` | Email de l'admin user |
| RDV pris | `has_booking_callN` | Tous statuts confondus, **annulations incluses** |
| RDV proposé | `has_booking_proposal_callN` | SMS contenant un lien Calendly sur la fenêtre de la session |

## Déroulé de l'analyse

Suivre cet ordre. Chaque étape a son fichier dans `sql/`. Le filtre de cohorte est la
ligne marquée `-- PARAM` en tête de chaque fichier.

**1. Cadrer — `sql/01_stats_par_session.sql`**
Taux d'appels OK, taux de PM et couverture par cohorte × session. Toujours commencer
là : donne le niveau absolu et repère les cellules anormales.

**2. Comparer au projeté — `sql/02_projete_vs_realise.sql`**
Projection = performance de chaque accompagnante de la cellule sur les **13 mois
précédant la fin de la session**, même session, cohorte exclue, pondérée par son volume
dans la cellule. L'écart réalisé − projeté isole ce qui n'est pas imputable au niveau
habituel des personnes ni à leur répartition.
`sql/02b_projete_vs_realise_par_accompagnante.sql` donne la même chose au grain
accompagnante : qui s'écarte le plus de sa propre base, en points et en volume.

**3. Décomposer — `sql/03_contributions_accompagnantes.sql`**
Contribution d'une accompagnante = `part du volume × écart individuel`. La somme des
contributions d'une cellule redonne exactement son écart. Répond à « qui pèse » et
« est-ce réparti ou localisé ».

**4. Résumer — `sql/04_resume_sous_performance.sql`**
Par cohorte : combien d'accompagnantes sous leur projection et quelle part des appels
elles portent.

**5. Mesurer le dispositif RDV — `sql/06_rdv_booking.sql` puis `sql/07_effet_rdv_adoption.sql`**
Le RDV a été déployé **en cours d'année 2026** (premiers RDV en février, 0 % de RDV au
call 0 en janvier contre 46 % en septembre). Toute comparaison entre cohortes doit en
tenir compte. `06` donne les taux de proposition, de prise de RDV et de transformation ;
`07` estime l'effet réel du dispositif hors biais de sélection. Lire le piège ci-dessous
avant d'interpréter quoi que ce soit sur les RDV.

**6. Diagnostiquer — `references/interpretation.md`**
Huit hypothèses à tester dans un ordre précis, avec les requêtes courtes associées et
les repères chiffrés pour trancher. Contient aussi la piste ouverte sur les
transcriptions Aircall, à reprendre dès que la table sera alimentée. `sql/05_projection_avec_canal.sql` quantifie la
part de l'écart imputable au mix de recrutement — le facteur le plus souvent en cause.
Le même fichier récapitule ce qui a déjà été établi sur ce périmètre, à ne pas refaire.

## Seuils de lecture

- **Écart individuel** : en dessous de ±5 points, c'est du bruit. Le seuil de −5 pts
  filtre correctement ; à −3 pts on ramasse du bruit, à −8 pts on ne voit plus que les
  cas extrêmes.
- **Volume minimum** : 100 appels pour classer une accompagnante sur une période, 15
  pour la compter dans une cellule. En dessous, l'écart est dominé par l'aléa.
- **Réparti ou localisé** : top 3 des contributeurs < 50 % de l'écart **et** plus de la
  moitié des accompagnantes du même côté → chercher la cause ailleurs que dans les
  personnes. Top 3 > 65 % → regarder les trois noms.
- **Régime normal vs dégradé** : la part des appels traités par une accompagnante à
  −5 pts ou moins tourne autour de 6-17 % en régime normal, et monte à 46-63 % sur une
  cohorte en difficulté.

## Pièges

- **`supporter_name` est l'affectation actuelle**, pas celle du jour de l'appel. Si des
  familles sont réaffectées en cours d'accompagnement, une partie des appels est
  attribuée à la mauvaise personne. `stg_1001mots_app__versions` (paper_trail)
  permettrait de reconstruire l'historique — non fait à ce jour. Le signaler dans toute
  restitution nominative.
- **Un compte générique `contact@…` apparaît dans les données.** L'exclure ou le
  signaler.
- **`is_bilingue` NULL n'est pas une population, c'est un artefact.** Le champ est
  renseigné pendant l'appel 0, donc un NULL signale surtout que l'appel n'a pas abouti
  (48,7 % d'OK contre 72-75 % chez les renseignés). L'utiliser comme variable
  explicative sans écarter les NULL revient à expliquer le résultat par lui-même. Le
  bilinguisme en soi n'a aucun effet : 74,5 % pour les bilingues, 72,5 % pour les autres.
- **`answered` d'Aircall ne veut pas dire « quelqu'un a décroché ».** Il vaut vrai dans
  94,6 % des cas avec une médiane de 63 s. Pour la joignabilité, passer par `duration`.
- **Ne pas sur-expliquer la cohorte la plus basse.** La dispersion normale du résidu est
  de 6,3 points ; sur 41 cellules, le minimum attendu par hasard est d'environ −13. Deux
  hypothèses ont déjà été construites puis invalidées sur le dossier d'avril 2026.
- **La fenêtre de 13 mois retarde sur les tendances structurelles.** Le taux de PM en
  session 2 est passé de 40 % (2025) à 57 % (2026) : la projection produit donc un
  écart positif systématique de +9 à +19 pts sur ces cellules, qui mesure la
  progression et non une surperformance ponctuelle. Sur la session 0, où il n'y a pas
  de tendance, les écarts sont centrés et la lecture est directe.
- **Le différentiel avec RDV / sans RDV n'est pas l'effet du dispositif.** L'écart brut
  de 25-30 points est très majoritairement de la sélection : les familles qui prennent
  RDV sont déjà celles qui répondent. L'effet réel est d'environ **+7 points**
  (`sql/07`). Et le taux « hors RDV » ne se compare pas dans le temps : quand l'adoption
  monte, ce groupe devient un résidu négativement sélectionné, sa baisse apparente de
  70 % à 61 % sur 2026 est un artefact de composition. Ne pas en faire un indicateur.
- **La projection 13 mois ignore le dispositif RDV.** Une accompagnante de septembre est
  comparée à sa version de l'an dernier, qui n'avait pas cet outil — une part de l'écart
  positif des cohortes récentes vient de l'adoption, pas de l'exécution.
- **Écart ≠ niveau.** Une accompagnante peut progresser fortement (+10 pts) tout en
  restant la plus basse en absolu. Toujours afficher les deux colonnes.
- **Une cohorte en cours a une couverture incomplète.** Une cohorte à 83 % de couverture
  affiche un taux provisoire : la saisie n'est pas terminée et les derniers statuts
  saisis sont rarement des OK.

## Restitution

Dire ce que les données montrent, y compris quand cela contredit l'intuition de
départ — c'est arrivé plusieurs fois sur ce périmètre (l'érosion par session était un
artefact de dénominateur ; le creux d'avril n'était pas calendaire). Quand un résidu
reste inexpliqué, le dire et formuler l'hypothèse comme une hypothèse, avec ce qu'il
faudrait aller chercher pour la trancher.
