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
| Décroché à la 1ʳᵉ tentative | `nb_of_tries_callN = '1ère tentative'` | Saisi par l'accompagnante. **Renseigné seulement si l'appel a abouti** (4,2 % sur les KO) : rapporté à toutes les familles c'est le taux de décroché, rapporté aux seules jointes c'est le nombre d'essais nécessaires. Corroboré par Aircall `duration >= 120` — voir `sql/10` |
| RDV proposé | `has_booking_proposal_callN` | SMS portant un lien Calendly **du dispositif** (`utm_source=1001mots`). Session imputée par `utm_campaign=callN`, repli sur la fenêtre de dates |

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
`07` estime l'effet réel du dispositif hors biais de sélection. `sql/10` mesure le
**décroché téléphonique à la première tentative** — à ne pas confondre avec l'appel non
abouti, qui porte sur la session entière. `sql/11` donne la **ligne de base d'avant le
dispositif** : c'est elle le comparateur d'un groupe proposé, jamais les non proposés de
la même période. Lire le piège ci-dessous avant d'interpréter
quoi que ce soit sur les RDV.

**6. Décomposer entièrement — `sql/08_effet_pool.sql` puis `sql/09_decomposition_complete.sql`**
La chaîne complète : `réel = barre du vivier + effet pool + effet canal + résidu`,
additive et exacte, pour les deux métriques. `08` isole seul l'effet du pool
d'accompagnantes mobilisé.

**7. Diagnostiquer — `references/interpretation.md` et `sql/diagnostic/`**
Dix hypothèses à tester dans un ordre précis, avec les requêtes courtes associées et
les repères chiffrés pour trancher. Le dossier `sql/diagnostic/` contient les dix-sept
requêtes correspondantes, chacune portant en tête son résultat — la plupart sont des
hypothèses **écartées**, et c'est l'essentiel de leur valeur : ne pas les refaire. Son
README sert d'index et sa section « Ce qui reste » donne les contraintes qu'une
hypothèse nouvelle doit satisfaire.

Les transcriptions Aircall sont exploitables depuis le 2026-10-02 et ont tranché la
question laissée ouverte sur avril (`d13`) : les appels courts sont des **répondeurs**,
pas de mauvais interlocuteurs.

Commencer par `sql/diagnostic/d08_dispersion_du_residu.sql` : la dispersion normale du
résidu est de 6,3 points, donc une cohorte basse n'appelle pas forcément d'explication. `sql/05_projection_avec_canal.sql` quantifie la
part de l'écart imputable au mix de recrutement — le facteur le plus souvent en cause.
Le même fichier récapitule ce qui a déjà été établi sur ce périmètre, à ne pas refaire.

## Résultats déjà établis

`references/resultats-2026.md` porte la décomposition chiffrée des treize cohortes de
2026 et les verdicts par cohorte — instantané au 2026-09-29, à rafraîchir début 2027.
Le consulter avant de relancer une analyse sur une cohorte de 2026 : elle y est
probablement déjà.

## La règle des deux appels KO consécutifs — l'attrition est administrative

**L'application désengage automatiquement une famille après deux appels KO consécutifs.**
C'est une règle métier, pas un comportement des familles, et c'est elle qui produit
l'essentiel de l'attrition. Vérifiée sur données (`sql/diagnostic/d18`, blocs 6 et 7) :

| S0 | S1 | Familles | Couverture S2 |
|---|---|---|---|
| KO | **KO** | 234 | **26,5 %** |
| KO | OK | 212 | 94,8 % |
| OK | KO | 227 | **98,2 %** |
| OK | OK | 446 | 98,0 % |

**Un KO isolé ne coûte rien en couverture ; seule la séquence compte.** Conséquences :

- **Ne jamais conditionner sur le statut d'une seule session** pour lire l'attrition :
  agréger « S1 ratée » mélange (OK, KO) qui ne perd personne et (KO, KO) qui perd tout,
  et fabrique un faux résultat. Toujours croiser S0 × S1.
- **Le bon indicateur de ce qu'une cohorte a coûté est la part de double KO**, pas le taux
  d'OK d'une session. Sur 2026 elle prédit la couverture presque parfaitement : 11,0 % →
  87,4 % de couverture S2 pour `2026/01/26`, 26,5 % → 66,4 % pour `2026/04/06`.
- **Le coût d'une mauvaise session dépend entièrement de la précédente.** Une cohorte qui
  a raté sa session 0 joue toute sa taille sur la session 1. `2026/04/20` a la session 0
  la plus basse de l'année (49,0 %) et sauve sa taille parce que sa session 1 revient à
  60,2 % ; `2026/04/06`, moins spectaculaire, rate les deux et perd **la moitié de sa
  cohorte**. Toujours afficher la couverture à côté des taux.

## Seuils de lecture

- **Comparer à la moyenne de l'année, pas à zéro.** La projection se cale sur 13 mois
  glissants : si le dispositif progresse, toutes les cohortes affichent un écart positif.
  Sur 2026, la moyenne pondérée est de +1,5 point brut et +2,1 après canal.
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
  94,6 % des cas avec une médiane de 63 s. Pour la joignabilité, passer par la durée.
- **Préférer `answered_at` à `duration`.** La source `MOTS_DL.MOTS_APP.AIRCALL_CALLS`
  porte `ANSWERED_AT`, `ENDED_AT` et `MISSED_CALL_REASON`, **absents du modèle de
  staging**. `datediff(second, answered_at, ended_at)` donne la conversation hors
  sonnerie et lève la bimodalité de `duration`. La sonnerie elle-même est de 14,5 à
  16,2 s sur toutes les cohortes 2026 — un écart sur cette valeur signalerait un
  problème de ligne, pas de famille. `caller_id` est l'identifiant de l'**agent**, pas
  un numéro de ligne sortante : la source n'en contient aucun.
- **Un appel court n'est pas une conversation courte, c'est un répondeur.** Établi sur
  les transcriptions (`sql/diagnostic/d13`) : dans la bande 60-120 s, l'essentiel des
  appels sont des messageries vocales où l'accompagnante laisse un message. Ne jamais
  interpréter la bande 1-2 min comme « un échange qui tourne court ».
- **Les transcriptions sont un échantillon biaisé par la durée.** Couverture de 0 % sous
  20 s à 24-31 % au-delà de 5 min. Tout taux brut calculé dessus est optimiste de 5 à
  6 points ; repondérer par la distribution réelle des durées (`d13`, bloc 3). Les
  comparaisons **intra-classe de durée** sont valides, les niveaux bruts non.
- **Les messages de répondeur sont longs.** « Je ne suis pas là, laissez-moi un message.
  Une fois votre message enregistré… » dépasse 80 caractères : un classificateur fondé
  sur la longueur du texte de l'interlocuteur les compte comme des humains. Utiliser la
  liste de marqueurs calibrée en tête de `d13`, ne pas la raccourcir.
- **Ne pas sur-expliquer la cohorte la plus basse.** La dispersion normale du résidu est
  de 6,3 points ; sur 41 cellules, le minimum attendu par hasard est d'environ −13. Sur
  le seul dossier d'avril 2026, **dix hypothèses** ont été construites puis invalidées —
  dont la conclusion de septembre elle-même (le fichier CAF périmé). Avant d'en ouvrir
  une onzième, lire la section « Ce qui reste » du README de `sql/diagnostic/` : elle
  liste les six contraintes qu'une explication doit satisfaire ensemble, et la plupart
  des idées qui viennent spontanément y échouent en une ligne.
- **Ne pas traiter les deux cohortes d'avril 2026 comme un seul dossier.** Leurs
  trajectoires sont opposées : `2026/04/20` fait un trou isolé en session 0 puis revient
  dans la moyenne (49,0 → 60,2), `2026/04/06` s'enfonce en session 1 (57,3 → 46,3). Et
  leur source coupable n'est pas la même — CAF 93 pour l'une, CAF Aube pour l'autre
  (`sql/diagnostic/d07`).
- **La règle 1 vaut aussi entre sessions, et c'est là qu'on l'oublie.** Un `pct_ok` qui
  remonte de session en session peut n'être qu'un dénominateur qui se vide : `2026/04/06`
  passe de 46,3 à 62,0 % entre S1 et S3 pendant que sa couverture tombe de 89,9 à
  50,8 %. Rapportée aux familles inscrites, elle baisse sans interruption. Afficher
  **OK / inscrite** à côté de **OK / traitée** dès qu'on compare des sessions.
- **Pondérer les écarts par source, pas seulement les mesurer.** Un écart de 13 points
  sur une source qui pèse 69 % de la cohorte n'est pas comparable au même écart sur une
  source à 8 %. Et quand une source domine à ce point, dire qu'elle sous-performe est
  presque une reformulation du déficit de la cohorte : l'information est dans le
  **contraste avec les autres sources du même lot**.
- **Comparer des sessions de même rang quand on teste le calendrier.** Un appel de
  session 1 est un appel chaud, un appel de session 0 un premier contact depuis un numéro
  inconnu : une période creuse peut peser sur l'un sans toucher l'autre. Le test
  calendaire de 2026 avait cette faiblesse et a dû être refait (il tient toujours).
- **La fenêtre de 13 mois retarde sur les tendances structurelles.** Le taux de PM en
  session 2 est passé de 40 % (2025) à 57 % (2026) : la projection produit donc un
  écart positif systématique de +9 à +19 pts sur ces cellules, qui mesure la
  progression et non une surperformance ponctuelle. Sur la session 0, où il n'y a pas
  de tendance, les écarts sont centrés et la lecture est directe.
- **`nb_of_tries_callN` n'existe que si l'appel a abouti.** C'est une rubrique du compte
  rendu d'appel : renseignée à 99,5 % sur les statuts OK et 96,2 % sur « Incomplet », mais
  à **4,2 % sur les KO** et 0 % sur « Numéro erroné ». Calculer « % 1ère tentative » sur les
  seules lignes renseignées répond à « quand on joint la famille, combien d'essais ? »
  (51,3 %) et non à « combien de familles décrochent au premier appel ? » (36,1 %). Les deux
  sont utiles, l'écart est de 15 points, afficher lequel on montre. Le champ échappe au piège
  PaperTrail de `stg_1001mots_app__child_supports` : 11 désaccords sur 34 202 familles.
- **`has_booking_proposal` ne se lit pas avant février 2026.** La règle a été resserrée
  le 2026-09-29 : un simple `body ilike '%calendly%'` ramassait trois choses distinctes.
  D'abord les **Calendly personnels de quelques accompagnantes** (de la forme
  `calendly.com/<prénom>1001mots` ou `calendly.com/<nom>`), utilisés de leur propre
  initiative dès 2024 — d'où
  des taux de proposition de 45 à 49 % sur 2024/06 et 2024/09 avec **0 % de RDV pris**,
  ces RDV ne remontant jamais dans `scheduled_calls`. Ensuite les SMS de **confirmation et
  de rappel** (« Vous avez RDV le… », « Rappel : vous avez RDV demain avec… »), qui portent
  aussi un lien Calendly mais sont la conséquence d'un RDV pris, pas une proposition. Le
  marqueur `utm_source=1001mots` isole les seules vraies invitations du dispositif et
  apparaît en février 2026, exactement au lancement. Effet du resserrage : 4 776 → 3 456
  familles jamais proposées, et les cohortes d'avant 2026 repassent à 0 %.
- **La proposition est proactive, pas un rattrapage.** Le SMS part en moyenne **3 jours
  avant l'ouverture de la fenêtre d'appel** et **99 % des propositions précèdent le moindre
  appel** (0,03 tentative en amont en moyenne). L'hypothèse « le lien est envoyé après un
  premier échec de contact » a été construite puis **invalidée** sur les horodatages Aircall
  — ne pas la reformuler. Conséquence : « proposé » et « non proposé » ne se distinguent pas
  par un historique d'appel, mais par l'accompagnante et la cohorte.
- **« Proposé sans RDV » est un résidu négativement sélectionné, et il ne l'est pas sur la
  joignabilité.** Au call 0 en régime établi, ce groupe fait 25,1 % de décroché brut contre
  35,2 % chez les non proposés — mais **43,6 % en conditionnel contre 42,9 % sur la ligne de
  base d'avant dispositif**, soit exactement la normale. Ils ne sont pas plus durs à joindre
  par appel : ils sont simplement moins nombreux à être joints (57,6 % contre 70,9 %), parce
  qu'ils ont déjà ignoré une sollicitation. Le bon comparateur d'un groupe proposé n'est
  jamais le groupe non proposé de la même période.
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
- **Ne jamais conclure sous 95 % de couverture.** Cas mesuré : 2026/09/07 affichait
  74,0 % d'appels OK et un résidu de +6,7 à 83 % de couverture ; une semaine plus tard,
  à 99,3 %, elle était à 68,7 % et +2,4. Soit 5,3 points d'écart sur le taux et 4,3 sur
  le résidu — assez pour transformer « la meilleure cohorte de l'année » en « dans la
  moyenne ». Les derniers statuts saisis ne sont quasiment jamais des OK.

## Restitution

Dire ce que les données montrent, y compris quand cela contredit l'intuition de
départ — c'est arrivé plusieurs fois sur ce périmètre (l'érosion par session était un
artefact de dénominateur ; le creux d'avril n'était pas calendaire). Quand un résidu
reste inexpliqué, le dire et formuler l'hypothèse comme une hypothèse, avec ce qu'il
faudrait aller chercher pour la trancher.
