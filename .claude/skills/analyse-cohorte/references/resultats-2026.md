# Résultats 2026 — décomposition des cohortes

Instantané au **2026-09-22**, au call 0 (les cohortes n'ont pas le même nombre de
sessions terminées, un taux « toutes sessions » ne se compare pas). Produit par
`sql/09_decomposition_complete.sql`.

    réel = barre du vivier + effet pool + effet canal + résidu

La chaîne est additive et exacte à l'arrondi près.

## Taux d'appels OK

| Cohorte | Barre vivier | Pool | Canal | Résidu | Réel | % RDV pris | % appels chez adoptantes |
|---|---|---|---|---|---|---|---|
| 2026/01/12 | 65,6 | -2,8 | +1,2 | **+6,3** | 70,3 | 0,0 | 21 |
| 2026/01/26 | 65,9 | -0,6 | +1,1 | **+2,7** | 69,1 | 0,0 | 0 |
| 2026/02/09 | 66,0 | +0,5 | -0,3 | **-2,6** | 63,6 | 0,0 | 18 |
| 2026/02/23 | 66,0 | -2,4 | +1,4 | **-0,7** | 64,3 | 2,9 | 8 |
| 2026/03/09 | 65,7 | -1,9 | +0,2 | **+3,3** | 67,3 | 5,3 | 26 |
| 2026/03/23 | 65,7 | +1,6 | -1,2 | **-2,0** | 64,1 | 3,3 | 5 |
| 2026/04/06 | 65,7 | -1,0 | -3,4 | **-4,0** | 57,3 | 10,3 | 34 |
| 2026/04/20 | 65,4 | -0,7 | -4,1 | **-11,6** | 49,0 | 21,4 | 54 |
| 2026/05/04 | 64,2 | +0,5 | +0,4 | **+3,4** | 68,5 | 20,7 | 53 |
| 2026/05/18 | 64,5 | +0,9 | -0,5 | **-3,2** | 61,7 | 36,7 | 83 |
| 2026/06/01 | 64,3 | +0,7 | -2,0 | **+4,4** | 67,4 | 37,1 | 85 |
| 2026/08/24 | 65,1 | +0,4 | +3,2 | **+0,2** | 68,9 | 37,9 | 75 |
| 2026/09/07 | 65,4 | +2,7 | -0,8 | **+6,7** | 74,0 | 46,4 | 87 |

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
| 2026/08/24 | 63,6 | +3,6 | +1,0 | **-1,9** | 66,3 |
| 2026/09/07 | 63,8 | +1,7 | -0,2 | **+0,7** | 66,0 |

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

**Le pool pèse deux fois plus sur les missions que sur les appels OK** — 7,1 points
d'amplitude contre 5,5. Cohérent avec l'écart individuel entre accompagnantes : 17 points
d'écart-type sur les missions contre 7 sur les appels OK. C'est le seul levier où le
choix des accompagnantes compte vraiment.

**Le canal ne joue presque pas sur les missions** (−1,7 à +1,0) alors qu'il pèse jusqu'à
4 points sur les appels OK. Il détermine si on joint la famille, pas ce qu'on fait une
fois au téléphone.

**L'année entière tourne au-dessus de sa base 13 mois** : écart moyen pondéré de
**+1,5 point** brut, **+2,1** après neutralisation du canal. C'est la référence à
laquelle comparer une cohorte, **pas zéro**. Une cohorte à +2 fait la moyenne de l'année,
pas mieux.

## Ce qui est significatif

Avec 171 à 447 appels par cohorte au call 0, l'erreur-type est de ~2,8 points sur les
appels OK et ~3,4 sur les missions. Mais le seuil pertinent n'est pas là : la
**dispersion normale du résidu est de 6,3 points** (cf. `sql/diagnostic/d08`), dont
seulement 27 % de bruit d'échantillonnage. Sur 41 cellules, le minimum attendu par le
seul hasard est d'environ −13 points.

**Une seule cellule de 2026 sort vraiment : 2026/04/20 au call 0**, à −11,6 sur les
appels OK et −9,4 sur les missions — la seule cohorte où les deux métriques décrochent
ensemble. Tout le reste tient dans la dispersion normale, y compris les résidus positifs
de 2026/09/07 (+6,7) et 2026/01/12 (+6,3) que j'aurais volontiers présentés comme des
sur-performances.

## Verdicts par cohorte

**2026/08/24 se démonte entièrement.** Sur les appels OK, son bon chiffre vient du canal
(+3,2, recrutement à 14 % de CAF contre ~50 % la norme), résidu nul. Sur les missions, il
vient du pool (+3,6), résidu −1,9. Rien dans son résultat ne vient de son exécution — à
ne pas citer comme un succès opérationnel.

**2026/01/26 est l'inverse sur les missions.** Son 59,1 % paraît médiocre mais elle
partait avec le meilleur pool de l'année (+3,5) : son exécution est à −6,1, le plus
mauvais résidu missions de 2026. Le pool a masqué une contre-performance.

**2026/03/23 n'a rien à expliquer.** Son écart brut de −1,9 est intégralement imputable
au canal (70 % de CAF). Résidu à +0,1.

**2026/04/06 et 2026/04/20** — le canal en explique 3,4 et 4,1 points. Le reste est
documenté dans `references/interpretation.md` et reste sans cause établie.

**2026/05/18** partage la signature Aircall d'avril en plus atténué, et son 61,7 % global
repose sur 36,7 % de RDV pris.

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

## À rafraîchir

Les repères chiffrés de ce fichier et de `references/interpretation.md` datent de
septembre 2026. Ils servent de point de comparaison quelques mois puis dérivent —
à reprendre début 2027.
