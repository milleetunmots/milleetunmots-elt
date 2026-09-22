-- d10 | Joignabilite reelle, par la duree des appels Aircall.
-- Remplace le statut "Numero errone", inexploitable (0 % sur certaines cohortes,
-- 2,5 % sur 2026/08/24 qui est une BONNE cohorte, le retirer du denominateur
-- deplace 2026/04/20 de 49,1 a 49,8 %).
-- /!\ "answered" est inutilisable : vrai dans 94,6 % des cas, mediane 63 s. Il
-- signale que l'appel a abouti sur le reseau, pas qu'un humain a repondu.
--
-- Resultat (avril 2026) : aucun exces d'echec technique (0,3 % contre 0,4 %), et
-- PLUS de tentatives que la normale (2,76 contre 2,44). Le taux de decrochage est
-- identique (95,2 % contre 95,4 %) mais l'ecart se creuse avec la duree :
-- -3,8 pts a 30 s, -6,8 a 60 s, -9,6 a 120 s. Les appels aboutissent et ne
-- deviennent pas des conversations. Ceci INVALIDE l'hypothese des numeros morts.
-- A situation d'appel identique, l'ecart tombe a ~3 points : le residu est
-- presque entierement un effet de composition entre classes de conversation.

-- Chaque bloc est autonome : le lancer separement.

-- (1) Classes de joignabilite et taux d'OK dans chacune
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
select case when dmax <  0   then 'z. pas d aircall'
            when dmax < 10   then 'a. echec technique (<10s)'
            when dmax < 60   then 'b. pas de conversation'
            when dmax < 300  then 'c. conversation courte (1-5min)'
            else                  'd. conversation longue (>5min)' end classe,
       count_if(grp='CIBLE')  n_cible,
       round(100.0*count_if(grp='CIBLE'  and stt='OK')/nullif(count_if(grp='CIBLE'),0),1)  ok_cible,
       count_if(grp='AUTRES') n_autres,
       round(100.0*count_if(grp='AUTRES' and stt='OK')/nullif(count_if(grp='AUTRES'),0),1) ok_autres,
       round(avg(iff(grp='CIBLE',tentatives,null)),2)  tent_cible,
       round(avg(iff(grp='AUTRES',tentatives,null)),2) tent_autres
from j group by 1 order by 1

;

-- (2) Courbe par seuil, au niveau TENTATIVE (plus informative qu'un seuil unique)
with fam as (
  select st.family_id, left(st.cohort_name,10) coh, st.support_creation_date::date cree,
         st.is_bilingue bil, st.call_0_status stt,
         iff(left(st.cohort_name,10) in ('2026/04/06','2026/04/20'),'CIBLE','AUTRES') grp
  from fact.super_table st
  where st.cohort_name like '2026/%'
    and left(st.cohort_name,10) between '2026/01/12' and '2026/09/07'
    and nullif(trim(st.call_0_status),'') is not null),
ap as (
  select f.grp, f.coh, a.duration d, a.answered
  from staging.stg_1001mots_app__aircall_calls a
  join fam f on f.family_id = a.child_support_id
  where a.call_session = 0 and a.direction = 'outbound')
select grp, count(*) tentatives,
       round(100.0*count_if(answered)/count(*),1) pct_answered,
       round(100.0*count_if(d>= 30)/count(*),1)   sup_30s,
       round(100.0*count_if(d>= 60)/count(*),1)   sup_60s,
       round(100.0*count_if(d>=120)/count(*),1)   sup_120s,
       round(100.0*count_if(d>=300)/count(*),1)   sup_300s
from ap group by 1 order by 1

;

-- (3) Meme courbe, par cohorte -- le seuil a 300 s ordonne les cohortes presque parfaitement
with fam as (
  select st.family_id, left(st.cohort_name,10) coh, st.support_creation_date::date cree,
         st.is_bilingue bil, st.call_0_status stt,
         iff(left(st.cohort_name,10) in ('2026/04/06','2026/04/20'),'CIBLE','AUTRES') grp
  from fact.super_table st
  where st.cohort_name like '2026/%'
    and left(st.cohort_name,10) between '2026/01/12' and '2026/09/07'
    and nullif(trim(st.call_0_status),'') is not null),
ap as (
  select f.coh, a.duration d, a.answered
  from staging.stg_1001mots_app__aircall_calls a
  join fam f on f.family_id = a.child_support_id
  where a.call_session = 0 and a.direction = 'outbound')
select coh, count(*) tentatives,
       round(100.0*count_if(answered)/count(*),1) pct_answered,
       round(100.0*count_if(d>= 60)/count(*),1)   sup_60s,
       round(100.0*count_if(d>=120)/count(*),1)   sup_120s,
       round(100.0*count_if(d>=300)/count(*),1)   sup_300s
from ap group by 1 order by 1
