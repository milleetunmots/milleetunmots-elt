# Diagnostiquer un écart de cohorte

Quand `02_projete_vs_realise.sql` montre une cellule nettement en dessous de sa
projection, **commencer par `sql/diagnostic/d08_dispersion_du_residu.sql`** : la
dispersion normale est de 6,3 points, et la cohorte la plus basse d'une année n'a pas
forcément besoin d'une explication. Puis tester les hypothèses **dans cet ordre**.
Chaque hypothèse a sa requête dans `sql/diagnostic/` — voir le README du dossier pour
l'index et les verdicts. Chacune est peu coûteuse et
plusieurs peuvent être écartées en une requête.

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

`stg_1001mots_app__aircall_calls` porte `duration` et `answered`. **`answered` est
inutilisable** : il vaut vrai dans 94,6 % des cas avec une médiane de 63 s — il signale
que l'appel a abouti sur le réseau, pas qu'un humain a répondu. Utiliser la **durée
maximale par famille × session** :

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

## 9. Les accompagnantes

Vient en **dernier**, pas en premier. `03_contributions_accompagnantes.sql` tranche :
si le top 3 porte moins de 50 % de l'écart et que plus de la moitié des accompagnantes
sont du même côté, le phénomène est collectif et la cause est ailleurs.

Sur `2026/04/20` S0 : 19 accompagnantes sur 22 en dessous de leur propre projection,
top 3 à 39 % seulement. Il n'y avait personne à désigner.

## 7. Le dispositif de RDV

Le RDV a été **déployé au cours de 2026** — premiers RDV en février, 0 % au call 0 en
janvier, 46,4 % en septembre. C'est un changement de dispositif en cours d'année : aucune
comparaison entre cohortes n'est valide sans en tenir compte.

Trajectoire au call 0 (`sql/06_rdv_booking.sql`) :

| Cohorte | % proposé | % RDV pris | Transformation |
|---|---|---|---|
| 2026/01/26 | 0 | 0 | — |
| 2026/03/09 | 24 | 5 | 22 % |
| 2026/04/20 | 54 | 21 | 40 % |
| 2026/06/01 | 83 | 37 | 45 % |
| 2026/09/07 | **85** | **46** | **55 %** |

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

À noter : **le RDV a amorti le choc d'avril** (−12,8 contre −23,0 sur 2026/04/20). Si
l'hypothèse du fichier périmé tient, c'est cohérent — un créneau posé donne un second
point de contact quand le numéro ne répond pas au hasard.

### Indicateurs à suivre

1. **Taux d'OK global** — le résultat, non biaisé, couvre tout le monde.
2. **Écart au projeté** — la performance nette du niveau habituel des personnes, avec la
   réserve que la projection ignore le dispositif RDV.
3. **Taux d'adoption de la proposition, par accompagnante** — le levier actionnable. Il
   reste du gisement : 3 accompagnantes à zéro sur 2026/09/07, 5 sur 2026/08/24.
4. **Taux de transformation proposition → RDV** — 22-36 % au printemps, 51-55 % depuis août.

Ne pas suivre le taux « hors RDV ».

---

# Piste ouverte : les transcriptions Aircall

Au 2026-09-22, `stg_1001mots_app__aircall_call_transcriptions` n'est pas encore
exploitable. Dès qu'elle l'est, c'est le test décisif du dossier avril.

**La question exacte** : sur les conversations de **1 à 2 minutes** des cohortes
`2026/04/06`, `2026/04/20` et `2026/05/18`, l'interlocuteur est-il le parent attendu ?

**Ce qu'elle départage** : deux hypothèses produisent la même signature dans les données
actuelles — des **numéros réattribués** (un tiers décroche, l'échange tourne court) ou
des **familles moins engagées** (le parent décroche et ne donne pas suite). Les durées
de 1 à 2 minutes plaident plutôt pour la seconde, un mauvais interlocuteur raccrochant
en 20-40 s, mais ce n'est pas tranché.

---

# Deux réglages de lecture

## Centrer sur la moyenne de l'année, pas sur zéro

La projection se cale sur 13 mois glissants. Si l'ensemble du dispositif progresse,
**toutes** les cohortes de l'année affichent un écart positif, et « +3 » n'est pas de la
sur-performance mais la dérive de l'année. Sur 2026 : écart moyen pondéré de **+1,5 pt**
brut et **+2,1** après neutralisation du canal. Une cohorte à +2 fait donc la moyenne,
pas mieux.

```sql
-- écart centré = écart de la cohorte - moyenne pondérée de l'année
round(ecart - sum(appels*ecart) over () / sum(appels) over (), 1) as ecart_centre
```

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
Résidu non expliqué de −11,7 pts (04/20 S0) et −9,5 pts (04/06 S1), dont la signature —
plus de KO, deux fois plus de numéros erronés, déficit concentré sur les enfants de
12 mois et plus, et **les familles ayant pris RDV qui ne répondent pas non plus**
(69,4 % d'OK contre 80-92 % partout ailleurs) — pointe vers la fraîcheur des coordonnées
du fichier CAF. Non vérifiable dans l'entrepôt : il faudrait la date de constitution du
fichier côté CAF.

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
