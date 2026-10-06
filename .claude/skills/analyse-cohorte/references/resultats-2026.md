# Résultats 2026 — décomposition des cohortes

Instantané au **2026-09-29**, au call 0 (les cohortes n'ont pas le même nombre de
sessions terminées, un taux « toutes sessions » ne se compare pas). Produit par
`sql/09_decomposition_complete.sql`.

    réel = barre du vivier + effet pool + effet canal + résidu

La chaîne est additive et exacte à l'arrondi près.

## Taux d'appels OK

| Cohorte | Barre vivier | Pool | Canal | Résidu | Réel | % RDV pris | % appels chez adoptantes |
|---|---|---|---|---|---|---|---|
| 2026/01/12 | 65,7 | -2,9 | +1,3 | **+6,2** | 70,3 | 0,0 | 21 |
| 2026/01/26 | 65,9 | -0,6 | +1,1 | **+2,7** | 69,1 | 0,0 | 0 |
| 2026/02/09 | 66,0 | +0,5 | -0,3 | **-2,6** | 63,6 | 0,0 | 18 |
| 2026/02/23 | 66,0 | -2,4 | +1,4 | **-0,7** | 64,3 | 2,9 | 8 |
| 2026/03/09 | 65,7 | -1,9 | +0,2 | **+3,3** | 67,3 | 5,3 | 26 |
| 2026/03/23 | 65,7 | +1,6 | -1,1 | **-2,1** | 64,1 | 3,3 | 5 |
| 2026/04/06 | 65,8 | -1,1 | -3,4 | **-4,0** | 57,3 | 10,3 | 34 |
| 2026/04/20 | 65,4 | -0,7 | -4,1 | **-11,6** | 49,0 | 21,4 | 54 |
| 2026/05/04 | 64,2 | +0,6 | +0,5 | **+3,2** | 68,5 | 20,7 | 53 |
| 2026/05/18 | 64,5 | +0,8 | -0,5 | **-3,1** | 61,7 | 36,7 | 83 |
| 2026/06/01 | 64,3 | +0,7 | -2,0 | **+4,4** | 67,4 | 37,1 | 85 |
| 2026/08/24 | 65,1 | +0,5 | +3,1 | **-0,1** | 68,6 | 37,9 | 77 |
| 2026/09/07 | 65,4 | +1,5 | -0,6 | **+2,4** | 68,7 | 43,5 | 87 |

## Taux de petites missions

| Cohorte | Barre vivier | Pool | Canal | Résidu | Réel |
|---|---|---|---|---|---|
| 2026/01/12 | 62,6 | -3,5 | -1,4 | **-1,9** | 55,8 |
| 2026/01/26 | 62,5 | +3,5 | -0,8 | **-6,1** | 59,1 |
| 2026/02/09 | 62,5 | +2,5 | -0,5 | **-1,5** | 63,0 |
| 2026/02/23 | 62,8 | -1,0 | +0,2 | **+2,5** | 64,5 |
| 2026/03/09 | 62,8 | -0,9 | +0,5 | **-2,7** | 59,7 |
| 2026/03/23 | 62,5 | +0,0 | -1,7 | **+0,2** | 61,0 |
| 2026/04/06 | 62,3 | -2,5 | -1,7 | **-1,0** | 57,1 |
| 2026/04/20 | 62,1 | +2,2 | -1,4 | **-9,4** | 53,5 |
| 2026/05/04 | 62,1 | +0,5 | +0,2 | **+2,3** | 65,1 |
| 2026/05/18 | 62,4 | +0,9 | +1,0 | **+4,9** | 69,2 |
| 2026/06/01 | 62,6 | +1,6 | -0,6 | **+3,6** | 67,2 |
| 2026/08/24 | 63,6 | +3,9 | +1,0 | **-1,9** | 66,6 |
| 2026/09/07 | 63,8 | +1,4 | -0,1 | **-0,5** | 64,6 |

## Le gradient entre sessions

| Session | Barre vivier | Réel OK | Réel missions | % RDV pris | Écart vs pool |
|---|---|---|---|---|---|
| **0** | 65,2 | **65,6** | 62,8 | 20,8 | **+0,2** |
| 1 | 58,1 | 61,1 | **63,1** | 16,6 | +2,9 |
| 2 | 57,7 | 59,7 | 56,6 | 19,9 | +1,2 |
| 3 | 59,2 | 63,8 | 52,5 | **32,9** | +3,7 |

La session 0 est environ **7 points au-dessus des suivantes sur les appels OK**, mais
**cet avantage est déjà dans la barre du vivier** (65,2 contre 57,7 au call 2) : la
projection compare chaque accompagnante à son propre historique **sur la même session**,
donc le gradient est neutralisé par construction et ne pollue pas la décomposition.

Le gradient n'est pas le même selon la métrique : **sur les missions, la session 1 fait
mieux que la session 0** (63,1 contre 62,8), et la chute est en fin de parcours (52,5 au
call 3). Ne pas généraliser « le call 0 est toujours meilleur ».

À noter : l'écart au pool est le **plus faible au call 0** (+0,2 contre +2,9 à +3,7
ailleurs). Les tables ci-dessus, publiées au call 0, sont donc conservatrices.

Enfin, le RDV est bien plus utilisé en fin de parcours — 32,9 % de RDV pris en session 3
contre 20,8 % en session 0, et 81 % des appels chez des accompagnantes adoptantes contre
49 %. Le dispositif sert surtout à rattraper les familles en fin d'accompagnement.

## Lecture

**La barre du vivier est stable toute l'année** — 64,2 à 66,0 sur les appels OK, 62,1 à
63,8 sur les missions. Si les appels avaient été répartis sur toutes les accompagnantes
au prorata de leur capacité, chaque cohorte aurait tourné dans une bande de moins de
2 points. La capacité moyenne de l'équipe n'a pas bougé : tout ce qu'on observe vient du
pool choisi, du canal, ou de ce qui s'est passé pendant la cohorte.

**Le pool pèse nettement plus sur les missions que sur les appels OK** — **7,4 points**
d'amplitude (−3,5 à +3,9) contre **4,5** (−2,9 à +1,6), soit 1,6 fois plus. Cohérent avec
l'écart individuel entre accompagnantes : 17 points d'écart-type sur les missions contre
7 sur les appels OK. C'est le seul levier où le choix des accompagnantes compte vraiment.

**Le canal ne joue presque pas sur les missions** (−1,7 à +1,0) alors qu'il pèse jusqu'à
4 points sur les appels OK. Il détermine si on joint la famille, pas ce qu'on fait une
fois au téléphone.

**L'année entière tourne au-dessus de sa base 13 mois** : écart moyen pondéré de
**+1,5 point** brut, **+2,1** après neutralisation du canal. C'est la référence à
laquelle comparer une cohorte, **pas zéro**. Une cohorte à +2 fait la moyenne de l'année,
pas mieux.

## Ce qui est significatif

Avec 171 à 547 appels par cohorte au call 0, l'erreur-type est de ~2,8 points sur les
appels OK et ~3,4 sur les missions. Mais le seuil pertinent n'est pas là : la
**dispersion normale du résidu est de 6,3 points** (cf. `sql/diagnostic/d08`), dont
seulement 27 % de bruit d'échantillonnage. Sur les 42 cellules de 2026, le minimum attendu par
le seul hasard est d'environ −13 points.

**Une seule cellule de 2026 sort vraiment : 2026/04/20 au call 0**, à −11,6 sur les
appels OK et −9,4 sur les missions — la seule cohorte où les deux métriques décrochent
ensemble. Tout le reste tient dans la dispersion normale, y compris le résidu positif
de 2026/01/12 (+6,2) qu'on aurait volontiers présenté comme une sur-performance.

**Leçon du 2026-09-29 : ne jamais publier une cohorte dont la couverture n'est pas
complète.** Au 22 septembre, 2026/09/07 affichait 74,0 % d'appels OK et un résidu de
**+6,7**, présentés ici comme la seule vraie sur-performance de l'année — sur une
couverture de 83 %. Une semaine plus tard, couverture à 99,3 %, elle est à **68,7 %** et
son résidu à **+2,4**, soit la moyenne de l'année. Les 94 statuts saisis en dernier
n'étaient quasiment pas des OK. Le taux de RDV pris a lui aussi reculé, de 46,4 % à
43,5 %. **Attendre 95 % de couverture avant de conclure quoi que ce soit.**

## Verdicts par cohorte

**2026/08/24 se démonte entièrement.** Sur les appels OK, son bon chiffre vient du canal
(+3,1, recrutement à 14 % de CAF contre ~50 % la norme), résidu nul. Sur les missions, il
vient du pool (+3,9, le meilleur de l'année), résidu −1,9. Rien dans son résultat ne vient de son exécution — à
ne pas citer comme un succès opérationnel.

**2026/01/26 est l'inverse sur les missions.** Son 59,1 % paraît médiocre mais elle
partait avec un pool très favorable (+3,5, deuxième de l'année derrière 2026/08/24) :
son exécution est à −6,1, le plus mauvais résidu missions de 2026. Le pool a masqué une
contre-performance.

**2026/03/23 n'a rien à expliquer** à 13 mois : son écart brut de −1,9 est intégralement
imputable au canal (70 % de CAF), résidu à −0,1. Réserve : avec une fenêtre de projection
de 6 mois, son écart passe à −5,9, ce qui la rapproche des cohortes d'avril. À regarder
si le dossier d'avril se précise.

**2026/04/06 et 2026/04/20** — le canal en explique 3,4 et 4,1 points. Le reste est
documenté dans `references/interpretation.md` et reste sans cause établie.

**Mise à jour du 2026-10-02 : les deux cohortes d'avril ne sont pas le même dossier.**
Les publier ensemble était une erreur de lecture. Leurs trajectoires divergent :

| Cohorte | S0 | S1 | S2 | S3 |
|---|---|---|---|---|
| 2026/04/06 | 57,3 | **46,3** | 57,6 | 62,0 |
| 2026/04/20 | **49,0** | 60,2 | 52,7 | — |

`2026/04/20` fait un **trou isolé sur son premier contact** puis revient dans la moyenne
de l'année en session 1 (60,2 contre 61,1 de moyenne S1), avec les mêmes familles et les
mêmes numéros. `2026/04/06` fait l'inverse : elle s'enfonce en session 1. Conséquence
directe : l'hypothèse du fichier CAF périmé, qui était la conclusion de septembre, est
**abandonnée** — des coordonnées mortes ne redeviennent pas valides en treize jours.
Les trois mesures qui la contredisent sont dans `sql/diagnostic/d13` à `d15`.

### Piège : la remontée de 2026/04/06 en S2-S3 est un artefact de dénominateur

Le tableau ci-dessus est en `pct_ok`, donc sur les seules familles au statut renseigné.
Lu comme ça, `2026/04/06` semble se rétablir (46,3 → 57,6 → 62,0). **Sa couverture
s'effondre en même temps** : 97,5 → 89,9 → 66,4 → 50,8 %. Rapporté à toutes les familles
inscrites, elle ne cesse de baisser. C'est exactement la règle 1 du SKILL appliquée à la
comparaison entre sessions — toujours afficher les deux dénominateurs.

| Cohorte | Métrique | S0 | S1 | S2 | S3 |
|---|---|---|---|---|---|
| 2026/04/06 | couverture | 97,5 | 89,9 | **66,4** | **50,8** |
| | OK / inscrite | 55,9 | 41,6 | 38,2 | **31,5** |
| | mission / inscrite | 31,9 | 25,6 | 23,5 | 16,8 |
| 2026/04/20 | couverture | 99,7 | 94,2 | 77,7 | — |
| | OK / inscrite | 48,8 | **56,7** | 40,9 | — |
| | mission / inscrite | 26,1 | 31,6 | 22,0 | — |
| 2026/03/23 *(réf.)* | OK / inscrite | 62,9 | 57,7 | 44,8 | 38,1 |
| 2026/05/04 *(réf.)* | OK / inscrite | 67,5 | 59,9 | 48,7 | — |

Lecture : **seule `2026/04/20` se rétablit vraiment.** Son écart à `2026/05/04` passe de
18,7 points en S0 à **3,2 points en S1**, avant de se rouvrir à 7,8 en S2. `2026/04/06`
ne se rétablit jamais et perd **la moitié de sa cohorte** d'ici la session 3 (50,8 % de
couverture contre 63,5 % pour `2026/03/23`) : c'est elle qui coûte le plus en volume
d'accompagnement, alors que c'est `2026/04/20` qui affiche le taux le plus spectaculaire.

### Et `2026/04/06`, elle, est expliquée

Son attrition a une cause établie — voir `sql/diagnostic/d18_session1_charniere.sql`.
**L'application désengage automatiquement une famille après deux appels KO consécutifs.**
`2026/04/06` est la seule cohorte de 2026 à avoir raté ses **deux** premières sessions
(57,3 puis 46,3), donc la seule à envoyer **26,5 % de ses familles** dans cette règle — la
pire part de l'année, contre 11,0 à 18,8 % ailleurs. Toute la perte de volume vient de là.
Un KO isolé ne coûte rien : 98,2 % de couverture après une séquence (OK, KO).

Le trou est porté par **CAF Oise** (133 familles importées en 2 jours), qui fait une
session 0 normale à 59,4 % puis tombe à **39,8 % en S1** avant de remonter à 64,4 % en S2.
Le mécanisme n'est ni le fichier ni le désengagement : pendant la fenêtre S1 ces familles
écrivent **plus** que la moyenne (22,6 % contre 17-19 %) et sont appelées **plus**
(2,71 tentatives contre 2,37-2,51), mais **46,7 % seulement de celles qui écrivent
obtiennent un appel OK**, contre 62 à 83 % ailleurs. La lecture des SMS montre une chaîne
de reports qui ne converge pas dans les 13 jours, l'indisponibilité de l'accompagnante
comprise. C'est un problème de prise de rendez-vous — exactement ce que le dispositif RDV
adresse, et `2026/04/06` n'était qu'à 35,3 % de proposition au call 0.

**2026/05/18** partage la signature Aircall d'avril en plus atténué, et son 61,7 % global
repose sur 36,7 % de RDV pris.

**2026/09/07 est dans la moyenne, pas au-dessus.** Résidu à +2,4 sur les appels OK et
−0,5 sur les missions, une fois la saisie terminée. Elle garde les meilleurs indicateurs
d'adoption du dispositif — 84,8 % de propositions, 43,5 % de RDV pris, 51,3 % de
transformation — mais cela ne se traduit pas en sur-performance mesurable.

## Le dispositif RDV n'est pas dans la chaîne

**C'est une fonctionnalité récente** : premiers RDV en **février 2026** (12 ce mois-là),
54 en mars, 254 en avril, 652 en juin. Les trois premières cohortes de 2026 ont un taux
de RDV strictement nul. Toute comparaison janvier/septembre non corrigée est faussée par
ce déploiement.

L'ajouter en cinquième couche produit des chiffres faux : la projection du pool s'appuie
sur l'historique 13 mois, qui pour les cohortes récentes contient déjà leurs cohortes
RDV — on le retirerait deux fois. La colonne « % appels chez adoptantes » permet d'en
estimer une **borne haute** : part × +7 points (l'effet mesuré hors biais par `sql/07`),
soit jusqu'à +6,1 sur 2026/09/07 et 0 sur 2026/01/26. La valeur réelle est entre 0 et
cette borne, plus près du bas pour les cohortes tardives.

Conséquence : sur 2026/09/07, je ne sais pas dire si le +6,7 est de l'exécution ou du
dispositif.

## Entonnoir de la proposition de RDV (call 0)

Mesuré au 2026-09-29 avec la règle resserrée (`utm_source=1001mots`, cf. le piège
`has_booking_proposal` dans SKILL.md). Dénominateur = **toutes les familles inscrites**
de la cohorte, pas seulement celles au statut renseigné — l'envoi du SMS ne dépend pas
du statut. La colonne « appel 0 non abouti » (statut renseigné ≠ OK) suit la règle 1.
Ne pas la confondre avec le **décroché téléphonique**, traité plus bas.

| Cohorte | Familles | % proposé | % RDV pris | Transfo. | Appel 0 non abouti |
|---|---|---|---|---|---|
| 2026/01/12 | 212 | 0,0 | 0,0 | — | — |
| 2026/01/26 | 191 | 0,0 | 0,0 | — | — |
| 2026/02/09 | 297 | 0,0 | 0,0 | — | — |
| 2026/02/23 | 174 | 8,6 | 2,9 | 33,3 | 35,7 |
| 2026/03/09 | 327 | 9,2 | 5,2 | 56,7 | 32,7 |
| 2026/03/23 | 310 | 4,8 | 3,2 | 66,7 | 35,9 |
| 2026/04/06 | 238 | 35,3 | 10,5 | 29,8 | 42,7 |
| 2026/04/20 | 291 | 52,9 | 21,3 | 40,3 | 51,0 |
| 2026/05/04 | 357 | 52,7 | 20,7 | 39,4 | 31,5 |
| 2026/05/18 | 309 | 81,6 | 35,9 | 44,0 | 38,3 |
| 2026/06/01 | 405 | 83,0 | 36,8 | 44,3 | 32,6 |
| 2026/08/24 | 431 | 71,2 | 37,6 | 52,8 | 31,4 |
| 2026/09/07 | 551 | 82,0 | 43,6 | 53,1 | 31,3 |

**Volume total** : 3 456 familles se sont vu proposer un RDV au moins une fois, toutes
sessions terminées confondues, sur 6 731 familles exposées au dispositif (une session
close depuis le 2026-02-01), soit **51,3 %**. 1 685 en ont pris un.

**Avril vs septembre au call 0** : 45,0 % (529 familles, deux cohortes) contre 82,0 %
(551 familles, la seule cohorte de septembre dont le call 0 soit terminé).

**Appel 0 non abouti sur les cohortes du dispositif** (2026/04/06 → 2026/09/07,
2 582 familles) : **35,6 %**. Décomposé :

| Groupe | Familles | % OK | % non abouti |
|---|---|---|---|
| RDV pris | 823 | 85,2 | **14,8** |
| Proposé sans RDV | 981 | 50,5 | **49,5** |
| Non proposé | 778 | 59,5 | **40,5** |

Le groupe « proposé sans RDV » échoue **plus** que le groupe non proposé. Ce n'est pas
un effet négatif du lien : le SMS part en priorité vers les familles déjà difficiles à
joindre. Les deux écarts de ce tableau sont du ciblage et de la sélection, pas l'effet du
dispositif — qui reste estimé à **+7 points** par `sql/07`.

## Décroché à la première tentative

Question distincte de la précédente : **la famille a-t-elle répondu dès le premier appel
passé ?** Requête dans `sql/10_decroche_premiere_tentative.sql`.

La mesure de référence est **`nb_of_tries_call0`**, saisi par l'accompagnante dans le
compte rendu d'appel. Le champ résiste au piège PaperTrail de
`stg_1001mots_app__child_supports` : 11 désaccords entre staging et source sur 34 202
familles.

**Il n'est renseigné que quand l'appel a abouti** — 99,5 % sur les statuts OK, 96,2 % sur
« Incomplet », mais **4,2 % sur les KO** et 0 % sur « Numéro erroné ». D'où deux lectures,
à ne jamais mélanger :

- **brut** = `1ère tentative` / toutes les familles au statut renseigné. Les familles
  jamais jointes comptent comme non décrochées, ce qu'elles sont. C'est le taux de
  décroché au sens courant.
- **conditionnel** = `1ère tentative` / familles effectivement jointes. Répond à
  « quand on y arrive, combien d'essais faut-il ? ».

Sur les cohortes du dispositif (2026/04/06 → 2026/09/07, 2 550 familles au statut
renseigné) : **36,1 % en brut**, 51,3 % en conditionnel, 70,3 % de familles jointes.

| Groupe | Familles | % jointes | **Décroché 1ʳᵉ (brut)** | 1ʳᵉ (cond.) | 2ᵉ | 3ᵉ+ |
|---|---|---|---|---|---|---|
| RDV pris | 818 | 88,4 | **58,9** | 66,7 | 20,2 | 13,1 |
| Proposé sans RDV | 960 | 56,9 | **24,0** | 42,1 | 32,2 | 25,6 |
| Non proposé | 772 | 67,7 | **26,9** | 39,8 | 36,9 | 23,3 |

| Cohorte | Familles | % proposé | % jointes | Décroché 1ʳᵉ (brut) | 1ʳᵉ (cond.) |
|---|---|---|---|---|---|
| 2026/02/23 | 171 | 8,2 | 73,7 | 26,3 | 35,7 |
| 2026/03/09 | 321 | 9,0 | 71,0 | 30,5 | 43,0 |
| 2026/03/23 | 304 | 4,9 | 68,4 | 34,2 | 50,0 |
| 2026/04/06 | 232 | 34,5 | 61,2 | 16,8 | 27,5 |
| 2026/04/20 | 290 | 52,8 | 58,3 | 23,1 | 39,6 |
| 2026/05/04 | 352 | 52,3 | 73,9 | 36,4 | 49,2 |
| 2026/05/18 | 300 | 81,3 | 68,0 | 34,7 | 51,0 |
| 2026/06/01 | 402 | 82,8 | 73,1 | 42,0 | 57,5 |
| 2026/08/24 | 427 | 71,4 | 74,2 | 43,3 | 58,4 |
| 2026/09/07 | 547 | 81,9 | 74,2 | 41,7 | 56,2 |

Avril 2026 : **20,3 %** en brut (34,1 % en conditionnel, 59,6 % de jointes).
Septembre 2026 : **41,7 %** (56,2 % / 74,2 %). C'est le plus gros écart mesuré sur ce
périmètre, et avril reste le point bas de l'année sur cette métrique comme sur les autres.

### Corroboration Aircall

Refaite sur la durée des appels, sans toucher au déclaratif : `duration >= 120 s` sur la
première tentative sortante donne **35,3 %** contre 36,1 % en brut. Deux sources
indépendantes qui convergent — le déclaratif des accompagnantes n'est pas complaisant.

Le seuil est calibré sur le statut d'appel, qui tranche sans ambiguïté :

| Durée max de la session | % de familles au statut OK |
|---|---|
| < 30 s | 23,3 |
| 30-60 s | 14,6 |
| 60-90 s | 15,3 |
| 90-120 s | 16,4 |
| 2-5 min | 46,6 |
| > 5 min | **95,8** |

`answered` est inutilisable (vrai dans 94,6 % des cas). La durée Aircall inclut la
sonnerie, d'où une distribution bimodale : bosse de 20 à 80 s (sonnerie puis répondeur),
creux vers 90-120 s, puis queue plate de vraies conversations. **Limite** : un parent qui
décroche et raccroche en 20 s est indiscernable d'une sonnerie. Le seuil à 30 s donne
92,3 % mais ne mesure que le fait que la ligne sonne — ne jamais le publier comme un taux
de décroché.

Par Aircall, le nombre moyen de tentatives passe de 2,84 sur les cohortes sans dispositif
à 2,29 sur 2026/08/24, et le RDV pris économise **0,7 tentative** par famille (2,07 contre
2,75 pour les proposés sans RDV).

### Ce que ça dit du dispositif

Le RDV **double le décroché au premier essai** (58,9 % contre 24-27 %) et fait passer la
part de familles jointes de 57-68 % à 88 %. Le gradient par cohorte va dans le même sens :
le décroché brut monte de 26-34 % quand presque rien n'est proposé à 42-43 % quand 80 % le
sont. Mécanisme direct — l'appel tombe sur un créneau que la famille a choisi.

Mais une part de l'écart est de la sélection. L'effet net du dispositif reste celui estimé
par `sql/07`, **environ +7 points** sur le taux d'appels OK.

### La ligne de base d'avant dispositif

Ce qu'il faut comparer, ce n'est pas « proposé » contre « non proposé » à la même période,
mais le régime établi contre la situation d'avant. Call 0, familles au statut renseigné :

| Période | Familles | % proposé | % jointes | Décroché brut | Cond. | Tentatives | % OK |
|---|---|---|---|---|---|---|---|
| **A. avant dispositif** (2025/01 → 2026/01) | 6 316 | 0,2 | 70,9 | **30,4** | 42,9 | 2,75 | 65,9 |
| B. démarrage (2026/02 → 2026/04) | 1 615 | 18,0 | 67,0 | 26,9 | 40,2 | 2,82 | 61,0 |
| **C. régime établi** (2026/05 → 2026/09) | 2 028 | 74,7 | 73,0 | **40,1** | 55,0 | 2,41 | 67,4 |

Décomposition du régime établi, avec les non proposés comme contrôle contemporain :

| Groupe | Familles | % jointes | Décroché brut | Cond. | Tentatives | % OK |
|---|---|---|---|---|---|---|
| C1. RDV pris | 701 | 89,3 | 61,2 | 68,5 | 2,02 | 86,6 |
| C2. Proposé sans RDV | 813 | 57,6 | 25,1 | **43,6** | 2,75 | 51,2 |
| C3. Non proposé | 514 | 75,3 | 35,2 | 46,8 | 2,39 | 66,7 |

Trois lectures que ce tableau permet et que le seul contraste proposé / non proposé
interdisait :

1. **Le gain principal est de l'efficacité, pas du taux de réussite.** Entre A et C, le
   décroché au premier essai prend **+9,7 points** et le nombre de tentatives baisse de
   0,34 (2,75 → 2,41), pendant que le taux d'appels OK ne bouge que de **+1,5 point**
   (65,9 → 67,4). Le dispositif fait surtout gagner des coups de fil.
2. **Le groupe « proposé sans RDV » n'est pas plus dur à joindre — il est moins souvent
   joint.** Son décroché conditionnel, 43,6 %, est celui de la ligne de base (42,9 %). Ce
   qui s'effondre, c'est la part de familles jointes : 57,6 % contre 70,9 %. C'est un
   résidu négativement sélectionné sur l'engagement, pas sur la joignabilité.
3. **Les non proposés de la période C ne sont pas un groupe privilégié** — 35,2 % de
   décroché brut et 66,7 % d'OK, à peine au-dessus de la ligne de base. Le contraste
   proposé / non proposé mesure donc bien de la sélection, et non un groupe témoin dégradé.

**Limite** : A contre C est un avant/après brut, confondu avec tout ce qui a bougé entre
2025 et 2026 — pool d'accompagnantes, canal de recrutement, saisonnalité. La dispersion
normale du résidu est de 6,3 points (`sql/diagnostic/d08`), donc le +1,5 point sur le taux
d'OK n'est pas distinguable de zéro. Le +9,7 sur le décroché, lui, dépasse largement cette
bande.

## L'effet cumulé sur les missions posées

Les deux taux se multiplient, et c'est ce produit qui dit combien de familles ont
réellement reçu une mission au call 0 :

| Cohorte | % appels OK | % PM | **% familles avec une mission** |
|---|---|---|---|
| 2026/04/20 | 49,0 | 53,5 | **26,2** |
| 2026/05/04 | 68,5 | 65,1 | **44,6** |

Soit **41 % de missions en moins en relatif** sur `2026/04/20`. Une cohorte qui décroche
sur les appels OK décroche deux fois sur les missions — toujours afficher ce produit en
restitution, pas seulement les deux taux côte à côte.

Et le déficit de PM n'est **pas** un simple effet de conversations plus courtes : en
appliquant les taux de `2026/04/20` à la composition de durée des autres cohortes, elle
passerait de 53,5 à 54,9 % seulement. La composition ne porte que ~1,4 point sur ~11. Le
reste est un déficit de conversion à durée égale, de 9 à 19 points sous 20 minutes de
conversation, **quasi nul au-delà** (71,1 contre 73,6). Détail dans
`sql/diagnostic/d17_missions_qualite_echange.sql`.

## À rafraîchir

Les repères chiffrés de ce fichier et de `references/interpretation.md` datent de
septembre 2026. Ils servent de point de comparaison quelques mois puis dérivent —
à reprendre début 2027.
