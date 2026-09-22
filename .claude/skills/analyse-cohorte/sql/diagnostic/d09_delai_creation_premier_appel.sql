-- d09 | La famille a-t-elle "oublie" l'accompagnement ?
-- Delai entre la creation de la fiche et le premier appel sortant Aircall.
-- Resultat (2026) : le delai NE PREDIT RIEN. Taux d'OK par tranche : 66,8 % a moins
-- d'un mois, 63,0 % a 1-2 mois, 69,0 % a 2-3 mois, 62,4 % a 3-5 mois et 71,6 % au-dela
-- de 5 mois -- le groupe le plus "oublie" est le meilleur.
-- Et les cohortes d'avril ont ete appelees PLUS VITE que les autres (delai median
-- 31 jours contre 39, 3,2 % de familles au-dela de 3 mois contre 16,2 %) tout en
-- faisant 15 points de moins de conversations longues. Hypothese ecartee.
--
-- /!\ LIMITE : support_creation_date est la date de creation de la fiche dans l'app,
-- donc la DATE D'IMPORT pour un lot. Elle ne mesure pas depuis quand la famille s'est
-- inscrite aupres du partenaire. Ce test ecarte "la famille a oublie depuis son
-- inscription dans l'app", PAS "le fichier partenaire etait ancien".

-- Chaque bloc est autonome : le lancer separement.

-- (1) Le delai predit-il la performance ?
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
j as (select f.*, coalesce(a.tentatives,0) tentatives, coalesce(a.dmax,-1) dmax,
             datediff(day, f.cree, a.premier_appel) delai
      from fam f left join ac a on a.fid = f.family_id)
select case when delai <  30 then 'a. <1 mois'  when delai <  60 then 'b. 1-2 mois'
            when delai <  90 then 'c. 2-3 mois' when delai < 150 then 'd. 3-5 mois'
            else 'e. >5 mois' end tranche,
       count(*) n,
       round(100.0*count_if(grp='CIBLE')/count(*),0)  pct_cible,
       round(100.0*count_if(stt='OK')/count(*),1)     pct_ok,
       round(100.0*count_if(dmax>=300)/count(*),1)    pct_conv_5min
from j where delai is not null group by 1 order by 1

;

-- (2) Distribution du delai dans le groupe evalue
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
j as (select f.*, coalesce(a.tentatives,0) tentatives, coalesce(a.dmax,-1) dmax,
             datediff(day, f.cree, a.premier_appel) delai
      from fam f left join ac a on a.fid = f.family_id)
select grp, count(*) n,
       round(median(delai),0)                        delai_median,
       round(avg(delai),0)                           delai_moyen,
       round(100.0*count_if(delai >= 90)/count(*),1) pct_plus_3mois,
       round(100.0*count_if(dmax>=300)/count(*),1)   conv_5min,
       round(100.0*count_if(stt='OK')/count(*),1)    pct_ok
from j where delai is not null group by 1 order by 1
