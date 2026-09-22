-- d11 | L'heure et le jour d'appel.
-- /!\ Les timestamps aircall sont en UTC : convert_timezone est indispensable.
-- Resultat (2026) : le creneau ne change presque rien -- 24,1 % de conversations
-- longues au pire creneau (18h+) contre 29,6 % au meilleur (12-14h), soit
-- 5,5 points d'amplitude. Trop peu pour porter un residu de 10 points.
-- Et le deficit d'avril est present dans TOUS les creneaux sans exception
-- (15,0 a 21,8 % contre 24,1 a 29,6 %). Hypothese ecartee.

with fam as (
  select st.family_id, left(st.cohort_name,10) coh, st.support_creation_date::date cree,
         st.is_bilingue bil, st.call_0_status stt,
         iff(left(st.cohort_name,10) in ('2026/04/06','2026/04/20'),'CIBLE','AUTRES') grp
  from fact.super_table st
  where st.cohort_name like '2026/%'
    and left(st.cohort_name,10) between '2026/01/12' and '2026/09/07'
    and nullif(trim(st.call_0_status),'') is not null),
ac as (
  select child_support_id fid, count(*) tentatives,
         max(duration) dmax, min(started_at)::date premier_appel
  from staging.stg_1001mots_app__aircall_calls
  where call_session = 0 and direction = 'outbound'
  group by 1),
ap as (
  select f.grp, f.coh, a.duration d,
         hour(convert_timezone('UTC','Europe/Paris', a.started_at))      h,
         dayname(convert_timezone('UTC','Europe/Paris', a.started_at))   jour
  from staging.stg_1001mots_app__aircall_calls a
  join fam f on f.family_id = a.child_support_id
  where a.call_session = 0 and a.direction = 'outbound')
select case when h < 10 then 'a. avant 10h' when h < 12 then 'b. 10-12h'
            when h < 14 then 'c. 12-14h'    when h < 16 then 'd. 14-16h'
            when h < 18 then 'e. 16-18h'    else 'f. 18h+' end creneau,
       count_if(grp='CIBLE')  n_cible,
       round(100.0*count_if(grp='CIBLE') /sum(count_if(grp='CIBLE'))  over (),1) part_cible,
       round(100.0*count_if(grp='AUTRES')/sum(count_if(grp='AUTRES')) over (),1) part_autres,
       round(100.0*count_if(grp='AUTRES' and d>=300)/nullif(count_if(grp='AUTRES'),0),1) conv5_autres,
       round(100.0*count_if(grp='CIBLE'  and d>=300)/nullif(count_if(grp='CIBLE'),0),1)  conv5_cible
from ap group by 1 order by 1
