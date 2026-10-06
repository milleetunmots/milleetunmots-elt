-- d17 | Le deficit de petites missions de 2026/04/20 est-il seulement la consequence
-- de conversations plus courtes ?
--
-- VERDICT (2026-10-02) : NON, et c'est le resultat utile. La composition n'explique
-- presque rien ; il y a un vrai deficit de conversion, concentre sur les echanges de
-- moins de 20 minutes et qui disparait au-dela.
--
-- Taux de PM par duree de conversation reelle (answered_at -> ended_at, cf. d14),
-- denominateur = appels OK :
--   | Duree       | poids 04/20 | poids autres | PM 04/20 | PM autres |
--   | < 5 min     |      18,9 % |       13,8 % |   37,0 % |    56,4 % |
--   | 5 - 10 min  |       9,8 % |       13,0 % |   35,7 % |    52,1 % |
--   | 10 - 20 min |      44,8 % |       38,2 % |   54,7 % |    64,1 % |
--   | > 20 min    |      26,6 % |       31,2 % |   71,1 % |    73,6 % |
--
-- Standardisation : en appliquant les taux de 2026/04/20 a la composition de duree
-- des autres cohortes, elle passerait de 53,5 % a 54,9 % seulement. La composition
-- ne porte donc que ~1,4 point d'un ecart d'environ 11. Les ~9,6 points restants
-- sont un deficit de conversion A DUREE EGALE, de 9 a 19 points sous 20 minutes,
-- quasi nul au-dela (71,1 contre 73,6).
--
-- Lecture operationnelle : un entretien qui atteint 20 minutes pose une mission au
-- taux normal, meme dans cette cohorte. Ce qui manque a 2026/04/20, ce sont les
-- entretiens longs -- et dans les entretiens courts, la mission passe a la trappe
-- bien plus souvent qu'ailleurs.
--
-- Effet cumule sur ce qui compte vraiment, la mission effectivement posee par
-- famille inscrite au call 0 :
--   2026/04/20 : 49,0 % d'OK x 53,5 % de PM = 26,2 % des familles
--   2026/05/04 : 68,5 % d'OK x 65,1 % de PM = 44,6 % des familles
-- soit 41 % de missions en moins en relatif. Les deux etages se multiplient : c'est
-- pour cela qu'une cohorte qui decroche sur les appels OK decroche deux fois sur les
-- missions. Toujours afficher ce produit en restitution, pas seulement les deux taux.
--
-- /!\ Ne pas lire ce tableau comme "il suffit de faire durer les appels". La duree
-- est en partie un marqueur de ce qui se passe dans l'echange, pas sa cause : un
-- parent presse donne un appel court ET pas de mission. La causalite n'est pas
-- etablie par cette requete.
--
-- PARAM : la cohorte evaluee, ligne 'CIBLE'.

with fam as (
  select family_id, left(cohort_name,10) coh, is_call0_goals pm,
         iff(left(cohort_name,10) = '2026/04/20','CIBLE','AUTRES') grp
  from fact.super_table
  where cohort_name like '2026/%' and left(cohort_name,10) between '2026/01/12' and '2026/09/07'
    and call_0_status = 'OK'),
ac as (
  select child_support_id fid, max(datediff(second, answered_at, ended_at)) conv_max
  from MOTS_DL.MOTS_APP.aircall_calls
  where call_session = 0 and direction = 'outbound' group by 1),
b as (
  select f.grp, f.pm,
         case when a.conv_max is null then 'z. pas d aircall'
              when a.conv_max < 300  then 'a. <5min'
              when a.conv_max < 600  then 'b. 5-10min'
              when a.conv_max < 1200 then 'c. 10-20min'
              else 'd. >20min' end classe
  from fam f left join ac a on a.fid = f.family_id),
agg as (
  select classe,
         count_if(grp='CIBLE')              n_c,
         avg(iff(grp='CIBLE',  pm, null))   pm_c,
         count_if(grp='AUTRES')             n_a,
         avg(iff(grp='AUTRES', pm, null))   pm_a
  from b group by 1)
select classe, n_c,
       round(100.0*n_c/sum(n_c) over (),1) poids_cible,
       round(100.0*n_a/sum(n_a) over (),1) poids_autres,
       round(100.0*pm_c,1) pm_cible,
       round(100.0*pm_a,1) pm_autres,
       -- taux de la cible standardise sur la composition des autres
       round(100.0*sum(n_a*pm_c) over ()/sum(iff(pm_c is null,0,n_a)) over (),1) pm_std
from agg order by 1
