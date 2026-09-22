# Diagnostiquer un écart de cohorte

Quand `02_projete_vs_realise.sql` montre une cellule nettement en dessous de sa
projection, tester les hypothèses **dans cet ordre**. Chacune est peu coûteuse et
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

Signature à chercher, **à canal constant** : hausse du KO, hausse des « Numéro erroné »,
taux de saisie inchangé (les appels ont bien été passés, ils n'ont pas abouti).

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

## 6. Les accompagnantes

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

# Ce qui a été établi sur le périmètre (à ne pas refaire)

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

**Le cas 2026/05/18**, qui résistait au diagnostic, s'explique par le RDV : son 61,7 %
global repose sur 36,7 % de RDV pris. Sur les familles des accompagnantes non
adoptantes, elle serait bien plus basse. C'est une cohorte faible dont le dispositif a
masqué la faiblesse.
