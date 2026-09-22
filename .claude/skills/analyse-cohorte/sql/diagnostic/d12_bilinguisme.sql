-- d12 | Bilinguisme et maitrise du francais.
-- Resultat (2026) : le bilinguisme n'est PAS un handicap -- 74,5 % d'appels OK pour
-- les familles bilingues contre 72,5 % pour les autres. Ce qui est coherent avec
-- l'existence de modules dedies.
--
-- /!\ PIEGE : les familles dont is_bilingue est NULL tombent a 48,7 %. Ce n'est pas
-- une population, c'est un ARTEFACT : le champ est renseigne PENDANT l'appel 0, donc
-- un NULL signale surtout que l'appel n'a pas abouti (68,8 % de conversations > 60 s
-- chez les NULL contre 84,5 % chez les renseignes). Ne jamais utiliser is_bilingue
-- comme variable explicative sans ecarter les NULL.
--
-- Il n'existe AUCUN champ structure sur la maitrise du francais. Les tags contenant
-- "bilingue" ou "parler" sont des campagnes ponctuelles de 2022-2023, pas un attribut
-- de famille. A creer cote applicatif si la dimension doit etre pilotee.

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
j as (select f.*, coalesce(a.dmax,-1) dmax from fam f left join ac a on a.fid=f.family_id)
select coalesce(bil,'(null)') bilingue, grp, count(*) n,
       round(100.0*count_if(stt='OK')/count(*),1)  pct_ok,
       round(100.0*count_if(dmax>=300)/count(*),1) pct_conv_5min,
       round(100.0*count_if(dmax>=60)/count(*),1)  pct_conv_60s
from j group by 1,2 order by 1,2
