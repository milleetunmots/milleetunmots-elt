-- d07 | LE test qui distingue "la source est mauvaise" de "le lot est mauvais".
-- Compare chaque source nommee DANS la cohorte evaluee et AILLEURS, a canal constant.
-- Resultat (2026/04/20) : CAF 93 a 47,7 % dans la cohorte contre 60,7 % ailleurs
-- (749 appels), CAF Oise 50,9 contre 59,3, CAF Aube 43,5 contre 55,8. Trois
-- departements differents, trois baisses simultanees de 8 a 13 points.
-- Ce n'est donc ni la CAF en general, ni un partenaire defaillant : c'est ce qui a
-- ete verse dans cette cohorte-la.
-- PARAM : la cohorte evaluee et le canal.

with b as (
  select st.source_name, st.call_0_status stt,
         iff(left(st.cohort_name,10) = '2026/04/20', 'CIBLE', 'AUTRES') grp
  from fact.super_table st
  where st.cohort_name like '2026/%'
    and st.source_channel = 'caf'
    and left(st.cohort_name,10) between '2026/01/12' and '2026/09/07'
    and nullif(trim(st.call_0_status),'') is not null)
select source_name,
       count_if(grp='CIBLE')                                                        n_cible,
       round(100.0*count_if(grp='CIBLE'  and stt='OK')/nullif(count_if(grp='CIBLE'),0),1)  ok_cible,
       count_if(grp='AUTRES')                                                       n_autres,
       round(100.0*count_if(grp='AUTRES' and stt='OK')/nullif(count_if(grp='AUTRES'),0),1) ok_autres
from b group by 1
having count_if(grp='CIBLE') >= 10
order by 2 desc
