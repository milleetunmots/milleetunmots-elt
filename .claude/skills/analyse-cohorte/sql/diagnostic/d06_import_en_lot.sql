-- d06 | Detecter un import en lot : concentration des dates de creation des familles.
-- Resultat (2026) : regime normal = 52 a 91 jours de creation distincts et 4 a 25 %
-- des familles sur le plus gros jour. 2026/04/06 etait a 26 jours et 56 % sur un
-- SEUL jour, 2026/05/18 a 49 %. Ce sont deux des trois cohortes en difficulte.
-- Indice, pas preuve : 2026/06/01 est a 30 % et se porte bien.

select left(cohort_name,10) coh,
       count(*)                                        n,
       count(distinct support_creation_date::date)     nb_jours_creation,
       round(100.0*max(c)/count(*),0)                  pct_plus_gros_jour,
       round(avg(datediff(day, support_creation_date::date,
                          try_to_date(left(cohort_name,10),'YYYY/MM/DD'))),0) delai_moyen_jours
from (select st.*,
             count(*) over (partition by left(cohort_name,10),
                                         support_creation_date::date) c
      from fact.super_table st
      where cohort_name like '2026/%')
group by 1 order by 1
