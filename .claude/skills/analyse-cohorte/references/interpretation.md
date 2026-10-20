# Diagnostiquer un écart de cohorte

Quand `02_projete_vs_realise.sql` montre une cellule nettement en dessous de sa
projection, **commencer par `sql/diagnostic/d08_dispersion_du_residu.sql`** : la
dispersion normale est de 6,3 points, et la cohorte la plus basse d'une année n'a pas
forcément besoin d'une explication. Puis tester les hypothèses **dans cet ordre**.
Chaque hypothèse a sa requête dans `sql/diagnostic/` — voir le README du dossier pour
l'index et les verdicts, dont les cinq ajouts du 2026-10-02 (`d13` à `d17`). Chacune est
peu coûteuse et plusieurs peuvent être écartées en une requête.

**Avant d'ouvrir une hypothèse nouvelle sur les cohortes d'avril 2026**, lire la section
« Ce qui reste » du README de `sql/diagnostic/` : elle liste les six contraintes que
toute explication doit satisfaire simultanément. Plusieurs hypothèses séduisantes y
échouent en une ligne.

## 1. Le calendrier (vacances, ponts, période creuse)

Lister les cellules par **date de fin de session** et comparer celles qui partagent la
même fenêtre. Si une autre cohorte a appelé les mêmes semaines avec les mêmes
accompagnantes et n'a pas décroché, le calendrier est écarté.

```sql
-- à partir du socle de 02, sans le filtre '2026/%' sur cible/volume/reel :
select left(p.cohort_name,10) coh, p.call_session s, r.fin,
       round(r.reel_ok - p.proj_ok, 1) ecart
from proj p join reel r using (cohort_name, call_session)
where p.n >= 50 and r.fin between '<debut>' and '<fin>'
order by r.fin
```

Exemple réel : au 3 mai 2026, `2026/03/23` S1 faisait +1,9 pendant que `2026/04/20` S0
faisait −15,7. Mêmes semaines, résultat opposé — hypothèse morte.

**Attention, cette comparaison-là était faible** (corrigé le 2026-10-02). Elle oppose une
session 1 à une session 0. Or un appel de session 1 est un appel **chaud** — la famille
connaît son accompagnante et a son numéro — quand un appel de session 0 est un premier
contact depuis un numéro inconnu. Des vacances ou un pont peuvent peser sur le second
sans toucher le premier : « mêmes semaines, résultats opposés » ne suffit donc pas, il
faut **comparer des sessions 0 entre elles**.

Refait correctement : sur la fenêtre 04/05 → 17/05, en plein pont du 1er et du 8 mai,
`2026/05/04` **S0** fait 68,5 %, le meilleur premier contact du printemps, pendant que
`2026/04/06` S1 fait 46,3 %. Et dans la fenêtre 20/04 → 03/05, `2026/04/20` S0 est à
49,0 % quand `2026/03/23` S1 est à 61,1 % et `2026/02/23` S2 à 67,6 %. **Le calendrier
reste écarté, maintenant sur la bonne comparaison.** Le tableau complet des fenêtres
réelles est dans `sql/diagnostic/d15_trajectoire_et_groupes_rdv.sql`, bloc 1 — s'en
servir plutôt que de reconstruire les dates à la main.

## 2. Le mix de recrutement

C'est le facteur le plus souvent en cause. Comparer les parts de canal :

```sql
select left(cohort_name,10) coh, count(*) n,
       round(100.0*count_if(source_channel='caf')/count(*),0)           pct_caf,
       round(100.0*count_if(source_channel='pmi')/count(*),0)           pct_pmi,
       round(100.0*count_if(source_channel='local_partner')/count(*),0) pct_partenaire
from fact.super_table where cohort_name like '2026/%' group by 1 order by 1
```

Écarts de performance par canal constatés sur 2026 (hors cohortes évaluées, dénominateur
appels avec statut) :

| Session | caf | pmi | local_partner |
|---|---|---|---|
| 0 | 62,2 | **72,1** | 71,2 |
| 1 | 59,2 | 65,6 | 66,2 |
| 2 | 56,3 | 63,4 | 70,7 |
| 3 | 64,3 | 63,5 | 63,8 |

Près de 10 points entre caf et pmi au call 0. Quantifier avec
`sql/05_projection_avec_canal.sql` : l'`ecart_residuel` est ce qui reste après
neutralisation du mix.

## 3. La charge de travail

Compter les appels que les accompagnantes de la cellule devaient passer sur la même
fenêtre, **toutes cohortes confondues** — une cellule peut être normale isolément et
tomber au milieu d'un pic de charge.

```sql
-- socle : a = (family_id, cohort_name, supporter_name, cs, deb, fin) distinct
-- puis charge = appels de la fenêtre / nb accompagnantes de la cellule
```

Repère 2026 : ~50 à 55 appels par accompagnante sur une fenêtre de 13 jours. Les
cohortes d'avril 2026 étaient à ~50, soit dans la fourchette **basse** — la charge a été
écartée.

## 4. La composition des familles

À canal constant, comparer tranche d'âge à l'inscription, bilinguisme, délai
d'inscription, département. Attention : un écart de composition ne prouve rien tant
qu'on n'a pas vérifié que la variable **discrimine** la performance sur le reste du
périmètre.

Cas réel : les cohortes d'avril 2026 avaient des enfants plus âgés (17 mois médians
contre 12), mais à canal caf identique elles **sur-performaient** sur les moins de
12 mois. La composition d'âge n'expliquait rien.

## 5. La qualité du fichier source

**Le statut `Numéro erroné` est inexploitable.** Il est renseigné de façon trop
irrégulière : 0 % sur certaines cohortes, 2,5 % sur 2026/08/24 qui est une *bonne*
cohorte. Le retirer du dénominateur déplace 2026/04/20 de 49,1 à 49,8 % — 0,7 point
sur un résidu de 11,6. Passer par Aircall à la place (voir ci-dessous).

Signature à chercher, **à canal constant** : hausse du KO, taux de saisie inchangé
(les appels ont bien été passés, ils n'ont pas abouti).

```sql
select grp, coalesce(nullif(trim(call_0_status),''),'(vide)') statut, count(*) n,
       round(100.0*count(*)/sum(count(*)) over (partition by grp),1) pct
from (select *, iff(left(cohort_name,10) in ('<cohorte>'),'CIBLE','AUTRES') grp
      from fact.super_table
      where cohort_name like '2026/%' and source_channel = '<canal>')
group by 1,2 order by 2,1
```

Croiser avec la concentration des dates de création, qui révèle un import en lot :

```sql
select left(cohort_name,10) coh, count(*) n,
       count(distinct support_creation_date::date) nb_jours_creation,
       round(100.0*max(c)/count(*),0) pct_plus_gros_jour
from (select st.*, count(*) over (partition by left(cohort_name,10),
                                  support_creation_date::date) c
      from fact.super_table st where cohort_name like '2026/%')
group by 1 order by 1
```

Repère : 52 à 91 jours de création distincts et 4 à 25 % sur le plus gros jour en
régime normal. `2026/04/06` était à 26 jours et **56 % sur un seul jour** — import en
lot caractérisé.

### Le bon indicateur de joignabilité : Aircall, pas le statut d'appel

**Mieux que `duration` : `answered_at`** (ajouté le 2026-10-02). La source
`MOTS_DL.MOTS_APP.AIRCALL_CALLS` porte `ANSWERED_AT`, `ENDED_AT` et
`MISSED_CALL_REASON`, qui **ne sont pas exposés par le modèle de staging**. La durée de
conversation hors sonnerie est donc calculable :
`datediff(second, answered_at, ended_at)`. Cela lève la limite documentée de `duration`
(qui inclut la sonnerie, d'où la distribution bimodale) et c'est la mesure à préférer
dans toute analyse nouvelle. Avec elle, sur le call 0 : `2026/04/20` a 23,2 % de
conversations de 2 min et plus contre 29,0 à 38,3 % ailleurs, et 17,8 % au-delà de 5 min
contre 24,1 à 31,8 %. Voir `sql/diagnostic/d14_numeros_vivants_sms.sql`.

La sonnerie, elle, est **identique partout** (14,5 à 16,2 s), ce qui écarte toute
anomalie de réseau ou de ligne appelante. À noter : la source ne porte **aucun numéro
appelant** — `caller_id` est l'identifiant de l'agent Aircall. L'hypothèse d'une ligne
sortante marquée comme spam n'est donc pas testable directement, seulement par la
sonnerie.

`stg_1001mots_app__aircall_calls` porte `duration` et `answered`. **`answered` est
inutilisable** : il vaut vrai dans 94,6 % des cas avec une médiane de 63 s — il signale
que l'appel a abouti sur le réseau, pas qu'un humain a répondu. Si l'on reste sur
`duration`, utiliser la **durée maximale par famille × session** :

| Classe | Seuil | Lecture |
|---|---|---|
| Échec technique | `dmax < 10 s` | Ligne morte. Repère : 0,3-0,4 % partout |
| Pas de conversation | `dmax < 60 s` | Sonnerie, répondeur |
| Conversation courte | 60 s – 5 min | Échange qui ne prend pas |
| Conversation longue | `dmax >= 300 s` | Le vrai appel. ~92-95 % d'OK |

La courbe par seuil est plus informative qu'un seuil unique : sur les cohortes d'avril,
le taux de décrochage est **identique** aux autres (95,2 % contre 95,4 %) et l'écart se
creuse avec la durée — **−3,8 pts à 30 s, −6,8 à 60 s, −9,6 à 120 s**. Les appels
aboutissent et ne deviennent pas des conversations. Cette signature **invalide
l'hypothèse des numéros morts** : les lignes répondent, et les accompagnantes ont
appelé davantage que la normale (2,76 tentatives contre 2,44).

## 6. La famille a-t-elle « oublié » l'accompagnement ?

**Testé, écarté.** Le délai entre la création de la fiche et le premier appel sortant
ne prédit rien : 66,8 % d'appels OK à moins d'un mois, 63,0 % à 1-2 mois, 69,0 % à
2-3 mois, 62,4 % à 3-5 mois et **71,6 % au-delà de 5 mois** — le groupe le plus
« oublié » est le meilleur.

Et les cohortes d'avril ont été appelées **plus vite** que les autres : délai médian de
31 jours contre 39, moyenne de 35 contre 56, et seulement 3,2 % de familles au-delà de
3 mois contre 16,2 %. Elles font quand même 15 points de moins de conversations longues.

**Limite à connaître** : `support_creation_date` est la date de création de la fiche
dans l'app, donc **la date d'import** pour un lot. Elle ne mesure pas depuis quand la
famille s'est inscrite auprès du partenaire. Ce test écarte « la famille a oublié depuis
son inscription dans l'app », il n'écarte pas « le fichier partenaire était ancien ».

Requête : `sql/diagnostic/d09_delai_creation_premier_appel.sql`.

## 7. L'heure et le jour d'appel

**Testé, sans effet.** La répartition horaire des appels est quasi identique entre
cohortes, et surtout le taux de conversation longue varie très peu selon le créneau :
24,1 % avant 18 h à 29,6 % entre 12 h et 14 h, sur les cohortes 2026 hors avril. Un
écart de 5 points entre le meilleur et le pire créneau ne peut pas porter un résidu de
10 points.

Sur les cohortes d'avril, le déficit est présent **dans tous les créneaux sans
exception** (15,0 à 21,8 % de conversations longues contre 24,1 à 29,6 %), ce qui
exclut un effet de composition horaire.

```sql
-- heure locale : les timestamps aircall sont en UTC
hour(convert_timezone('UTC','Europe/Paris', a.started_at))
```

## 8. Le bilinguisme

**Testé, sans effet — et attention au piège.** Sur les cohortes 2026, les familles
bilingues font **74,5 %** d'appels OK contre **72,5 %** pour les non bilingues : aucun
handicap, plutôt l'inverse. Le bilinguisme n'est pas un facteur de performance.

Le piège est ailleurs : les familles dont `is_bilingue` est **NULL** tombent à
**48,7 %**. Cette catégorie n'est pas une population, c'est un **artefact** — le champ
est renseigné pendant l'appel 0, donc un NULL signale surtout que l'appel n'a pas
abouti. Le corrélat le montre : 68,8 % de conversations dépassant 60 s chez les NULL
contre 84,5 % chez les renseignés. **Ne jamais utiliser `is_bilingue` comme variable
explicative sans écarter les NULL**, sinon on explique le résultat par lui-même.

Il n'existe par ailleurs **aucun champ structuré sur la maîtrise du français**. Les
tags contenant « bilingue » ou « parler » sont des campagnes ponctuelles, pas un
attribut de famille.

## 9. Le dispositif de RDV

Le RDV a été **déployé au cours de 2026** — premiers RDV en février, 0 % au call 0 en
janvier, 43,5 % en septembre. C'est un changement de dispositif en cours d'année : aucune
comparaison entre cohortes n'est valide sans en tenir compte.

Trajectoire au call 0 (`sql/06_rdv_booking.sql`) :

| Cohorte | % proposé | % RDV pris | Transformation |
|---|---|---|---|
| 2026/01/26 | 0 | 0 | — |
| 2026/03/09 | 24 | 5 | 22 % |
| 2026/04/20 | 54 | 21 | 40 % |
| 2026/06/01 | 83 | 37 | 45 % |
| 2026/09/07 | **85** | **44** | **51 %** |

Les deux leviers ont progressé : la proposition (pratique des accompagnantes) et la
transformation proposition → RDV.

### Le piège, et comment l'éviter

Les familles avec RDV affichent 82-90 % d'appels OK contre 54-61 % sans. **Cet écart
n'est pas l'effet du dispositif** : les familles qui prennent RDV sont déjà celles qui
répondent. Et le taux « hors RDV » ne se compare pas dans le temps — quand l'adoption
monte, ce groupe devient un résidu de plus en plus négativement sélectionné, sa baisse
de 70 % à 61 % sur l'année est un artefact.

La sortie par le haut : **la proposition est une pratique d'accompagnante, en tout ou
rien** — dans chaque cohorte la plupart sont à 0 % ou à ~100 %, presque personne entre
les deux. On compare donc, dans une même cohorte, la totalité des familles des
adoptantes à celle des non-adoptantes, chaque groupe mesuré par son écart à sa propre
projection 13 mois (`sql/07_effet_rdv_adoption.sql`). Aucune sélection au niveau famille,
et le niveau individuel des accompagnantes est neutralisé.

| Cohorte | Adoptantes | Non adoptantes | Différence |
|---|---|---|---|
| 2026/03/09 | +1,0 | +2,8 | −1,8 |
| 2026/04/06 | +1,2 | −11,7 | **+12,9** |
| 2026/04/20 | −12,8 | −23,0 | **+10,2** |
| 2026/05/04 | +3,7 | +0,1 | +3,6 |
| 2026/08/24 | +5,2 | −3,6 | **+8,8** |

**Effet réel : environ +7 points**, sur 4 cohortes comparables sur 5 — soit un quart de
l'écart brut. Effectifs modestes (60 à 360 appels par groupe, incertitude de 5 à 8 pts
par cohorte) : c'est la cohérence entre cohortes qui porte le résultat. Biais résiduel
non vérifié : la répartition des familles entre adoptantes et non-adoptantes au sein
d'une cohorte pourrait suivre le département ou la langue.

À noter : **le RDV a amorti le choc d'avril** (−12,8 contre −23,0 sur 2026/04/20) — c'est
le résultat le plus solide sur ce dossier et il tient toujours. Un créneau posé donne un
rendez-vous explicite là où l'appel tombe sur le répondeur.

Mais **ne pas en faire l'explication de la cohorte** (vérifié le 2026-10-02) : la masse
de l'écart de `2026/04/20` est dans le groupe **non proposé**, qui porte −10,7 des
−19,6 points mesurés contre `2026/05/04`, contre −4,5 pour le groupe ayant pris un RDV et
−4,4 pour les proposés sans RDV. Le dispositif ne peut porter qu'un quart de l'affaire.
Détail et vérifications dans `sql/diagnostic/d15_trajectoire_et_groupes_rdv.sql`.

### Indicateurs à suivre

1. **Taux d'OK global** — le résultat, non biaisé, couvre tout le monde.
2. **Écart au projeté** — la performance nette du niveau habituel des personnes, avec la
   réserve que la projection ignore le dispositif RDV.
3. **Taux d'adoption de la proposition, par accompagnante** — le levier actionnable. Il
   reste du gisement : 3 accompagnantes à zéro sur 2026/09/07, 5 sur 2026/08/24.
4. **Taux de transformation proposition → RDV** — 22-36 % au printemps, 51-55 % depuis août.

Ne pas suivre le taux « hors RDV ».

---

## 9 bis. La saisie du suivi de mission, et pourquoi 2025 n'est pas comparable à 2024

Établi le 2026-10-20, en construisant le suivi des indicateurs Fidelity.

**Toute comparaison du taux de missions réussies entre 2024 et 2025 est faussée
par la saisie.** Pour une petite mission posée dont l'appel suivant a abouti, le
suivi (`callN_previous_goals_follow_up`) est renseigné dans :

| Année de cohorte | Suivi saisi | Réussie si saisi | PM comptée comme réussie |
|---|---|---|---|
| 2023 | 57,6 % | 70,2 % | 25,9 % |
| 2024 | 74,2 % | 73,7 % | 36,4 % |
| 2025 | **89,9 %** | 81,1 % | 49,8 % |
| 2026 | 90,4 % | 82,1 % | — (cycles en cours) |

Une mission dont le suivi n'est jamais saisi ne peut pas compter comme réussie :
elle est indiscernable d'un échec. En isolant les trois facteurs, les
**+13,4 points** de probabilité qu'une mission compte entre 2024 et 2025 se
répartissent en **+7,7 points de saisie**, +3,7 points de réussite déclarée et
+1,0 point d'appels suivants aboutis.

Deux conséquences pratiques :

1. **Ne pas lire une hausse du taux de missions entre 2023, 2024 et 2025 comme
   une hausse de performance.** Elle mesure d'abord le remplissage du champ. À
   partir de 2025 la saisie plafonne à 90 % et la comparaison redevient propre.
2. **Un taux de missions agrégé sur une cohorte en cours est sous-estimé**, la
   mission se déclarant à l'appel suivant. Toute lecture annuelle doit se
   restreindre aux cohortes dont le cycle est achevé.

À noter : le KPI global du contrat Fidelity **ne mesure pas** la réussite des
missions, mais leur **pose** (`is_callN_goals`), qui n'est pas affectée par ce
biais — la pose de mission parmi les appels 0 aboutis passe de 85,7 % en 2024 à
92,7 % en 2025, et c'est une vraie généralisation de la pratique. Le piège
décrit ici ne concerne donc que les analyses de réussite des missions, pas le
suivi du financeur. Voir `fact.suivi_fidelity` pour les définitions du contrat.

---

## 10. Les accompagnantes

Vient en **dernier**, pas en premier. `03_contributions_accompagnantes.sql` tranche :
si le top 3 porte moins de 50 % de l'écart et que plus de la moitié des accompagnantes
sont du même côté, le phénomène est collectif et la cause est ailleurs.

Sur `2026/04/20` S0 : 19 accompagnantes sur 22 en dessous de leur propre projection,
top 3 à 39 % seulement. Il n'y avait personne à désigner.

# Piste refermée : les transcriptions Aircall ont tranché

**Fait au 2026-10-02.** La table `stg_1001mots_app__aircall_call_transcriptions` est
alimentée depuis avril 2026. Requêtes : `sql/diagnostic/d13_transcriptions_interlocuteur.sql`.

**La réponse** : ni numéros réattribués, ni mauvais interlocuteur. Les appels courts
d'avril sont des **répondeurs** — ce que la durée seule ne permettait pas de voir, et ce
qui explique la signature trouvée par `d10`. Trois résultats :

1. **À durée d'appel égale, avril n'est jamais en dessous** sur le taux d'appels où un
   humain répond : 13,1 % contre 15,6 % sur 20-60 s, 32,8 vs 36,5 sur 60-120 s, 94,4 vs
   78,7 sur 2-5 min, 98,8 vs 90,7 au-delà. L'écart agrégé (36,9 % contre 43,1 % de
   tentatives atteignant un humain) est **entièrement de la composition**.
2. **Quand le parent décroche, c'est le bon parent.** Zéro tiers sur les 36 échanges
   humains non-OK lus à la main.
3. Ce que ces échanges montrent à la place : le parent décroche, dit qu'il est au travail
   ou avec le bébé, **convient d'un rappel — et l'entretien n'a pas lieu** dans les
   13 jours.

**L'entonnoir fuit à chaque étage, modestement, et ça se cumule** :

| Groupe | familles jointes | dont sans entretien | OK après report | OK si entretien |
|---|---|---|---|---|
| Avril | 62,9 % | 32,7 % | 41,7 % | 90,5 % |
| Autres | 68,2 % | 23,9 % | 47,0 % | 96,5 % |

**Deux pièges à connaître avant de relancer quoi que ce soit sur les transcriptions**,
tous deux détaillés dans l'en-tête de `d13` : la couverture **croît avec la durée**
(0 % sous 20 s, 24-31 % au-delà de 5 min), donc tout taux brut calculé sur
l'échantillon est optimiste et il faut repondérer ; et les messages de répondeur
dépassent souvent 80 caractères, donc un classificateur fondé sur la longueur du texte
externe les compte comme des humains — utiliser la liste de marqueurs calibrée de `d13`.

---

# Trois réglages de lecture

## Centrer sur la moyenne de l'année, pas sur zéro

La fenêtre de 13 mois retarde volontairement sur la tendance — c'est le prix d'une
couverture à 100 %. Conséquence directe : l'année entière flotte au-dessus de zéro, et
il faut recentrer. Une fenêtre de 6 mois ramènerait la dérive de +2,38 à +0,74 mais
coûterait 7 points de couverture, sans gain de précision (écart-type 6,12 contre 6,32).
L'arbitrage est documenté dans l'en-tête de `sql/02_projete_vs_realise.sql`.

La projection se cale sur 13 mois glissants. Si l'ensemble du dispositif progresse,
**toutes** les cohortes de l'année affichent un écart positif, et « +3 » n'est pas de la
sur-performance mais la dérive de l'année. Sur 2026 : écart moyen pondéré de **+1,5 pt**
brut et **+2,1** après neutralisation du canal. Une cohorte à +2 fait donc la moyenne,
pas mieux.

```sql
-- écart centré = écart de la cohorte - moyenne pondérée de l'année
round(ecart - sum(appels*ecart) over () / sum(appels) over (), 1) as ecart_centre
```

## Le gradient entre sessions est déjà neutralisé

La session 0 fait environ 7 points de mieux que les suivantes sur les appels OK
(barre du vivier : 65,2 au call 0 contre 57,7 au call 2). **Ce gradient est absorbé par
construction** : la projection compare chaque accompagnante à son historique sur la
**même** session. Inutile de le corriger à la main.

Deux nuances : sur les **missions** le gradient s'inverse au call 1 (63,1 contre 62,8 au
call 0), donc ne pas généraliser ; et l'écart au pool est le **plus faible au call 0**
(+0,2 contre +2,9 à +3,7), donc une analyse publiée au call 0 est conservatrice.

## La pondération par les volumes : correcte, mais son apport propre est faible

Comparée à une moyenne simple des accompagnantes (chacune comptant pour une), la
projection pondérée ne bouge que de −0,8 à +1,7 point selon les cohortes de 2026. La
concentration est trop stable d'une cohorte à l'autre — la première accompagnante porte
toujours 10 à 18 % des appels — pour que la pondération fasse la différence entre
cohortes. On la garde parce qu'elle est juste, pas parce qu'elle est discriminante.

Exception utile : sur 2026/05/18 elle ajoute +1,7 à la barre, ce qui **aggrave** son
écart — ses gros volumes étaient tenus par des accompagnantes plutôt fortes.

---

# Ce qui a été établi sur le périmètre (à ne pas refaire)

Les résultats chiffrés cohorte par cohorte sont dans `references/resultats-2026.md`.

**L'effet accompagnante est réel, fort et persistant.** À cohorte et session égales,
écart-type de 6,7 pts sur les appels OK (étendue −14,1 à +12,4) et 17,2 pts sur les
missions. Il résiste aux contrôles de portefeuille (âge, bilinguisme, canal) : 5,3 et
13,8 pts. Et il est stable d'une année sur l'autre — la corrélation entre l'écart 2025
et l'écart 2026 d'une même personne est de **0,77** sur les appels OK et **0,85** sur
les missions, sur 34 à 36 accompagnantes.

**Mais il n'explique pas les écarts entre cohortes sur les appels OK.** Ce sont
largement les mêmes accompagnantes d'une cohorte à l'autre, leurs écarts se compensent :
le mix ne déplace une cohorte que de ±1,3 pt quand les écarts réels font ±5 pts.
**Sur les missions, en revanche, il compte** : corrélation de 0,72 (2025) et 0,43 (2026)
entre l'effet attendu du mix et l'écart réel, parce que l'écart individuel y est bien
plus large (de 11,9 % à 89,1 % de missions posées par appel OK).

**Le dispositif de RDV vaut environ +7 points**, pas les 25-30 de l'écart brut, et il a
été déployé en cours d'année 2026 — ce qui fausse toute comparaison janvier/septembre
non corrigée. Voir l'hypothèse 7 ci-dessus.

**La concentration des volumes est forte** : 11 à 19 % des appels d'une cohorte sur la
première accompagnante, 34 à 50 % sur les cinq premières. Toujours pondérer.

**Le cas des cohortes d'avril 2026** (−7,2 et −6,9 pts) : mix de recrutement à 96,8 % CAF
contre 49,2 % ailleurs, ce qui absorbe un quart à la moitié de l'écart. Calendrier,
charge, identité de la source CAF, composition d'âge et mix d'accompagnantes écartés.

**Reprise du 2026-10-02 — l'hypothèse du fichier CAF périmé est abandonnée.** Elle était
la conclusion de septembre, sur la foi de la signature Aircall. Trois mesures
indépendantes la contredisent : les appels courts sont des **répondeurs** et non des
tiers qui raccrochent (`d13`), les SMS arrivent et les familles y répondent au taux des
bonnes cohortes (`d14`), et surtout **`2026/04/20` remonte à 60,2 % en session 1** avec
les mêmes familles et les mêmes numéros (`d15`). Des coordonnées périmées ne
redeviennent pas valides en treize jours. Il n'est donc plus nécessaire d'aller chercher
la date de constitution du fichier côté CAF.

**Ce qu'il faut retenir à la place.** Le mécanisme est établi, la cause ne l'est pas, et
la liste de ce qui est écarté est maintenant assez longue pour que ce soit le résultat
lui-même. Les six contraintes que toute hypothèse nouvelle doit satisfaire ensemble sont
listées dans `sql/diagnostic/README.md`. Deux remarques pour cadrer la lecture :

- **Les deux cohortes d'avril n'ont pas le même profil** — ne plus les traiter en bloc.
  `2026/04/20` fait un trou isolé en session 0 (49,0) puis revient dans la moyenne
  (60,2 en S1). `2026/04/06` s'enfonce au contraire en session 1 (57,3 → 46,3).
- **La lecture la plus probable reste celle de `d08`** : −11,6 n'est qu'à 1,8 écart-type
  de la dispersion normale, et sur 42 cellules le minimum attendu par le seul hasard est
  d'environ −13. Il est possible qu'il n'y ait pas de cause unique à trouver.

Seule piste encore ouverte, et elle est bornée : la gestion des créneaux pendant la
montée en charge du dispositif RDV, qui porte **au plus 4,5 points sur 19,6** (`d15`).

**Le cas 2026/05/18**, qui résistait au diagnostic, s'explique en partie par le RDV :
son 61,7 % global repose sur 36,7 % de RDV pris. Elle partage par ailleurs la signature
Aircall d'avril, en plus atténué (23,6 % de conversations longues, troisième plus bas
de l'année) — ce qui la rattache au même phénomène.

**Mise en garde statistique.** La dispersion normale du résidu est de **6,3 points** sur
128 cellules, dont 27 % de la variance vient du seul bruit d'échantillonnage (249 appels
par cellule en moyenne). Un résidu de −11,6 n'est donc qu'à 1,8 écart-type, et sur
41 cellules le minimum attendu par le seul hasard est d'environ −13 points. **Ne pas
sur-expliquer une cohorte parce qu'elle est la plus basse** : sur ce dossier, deux
hypothèses ont déjà été construites puis invalidées (le calendrier, puis les numéros
morts).
