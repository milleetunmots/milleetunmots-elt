-- d14 | Les numeros d'avril sont-ils perimes ? Trois mesures independantes de l'appel.
--
-- VERDICT (2026-10-02) : ECARTEE, et cette fois sans ambiguite. Les numeros sont
-- vivants, les SMS arrivent, et les familles y repondent au taux normal.
--   * delivrabilite SMS (Aircall) sur la fenetre d'appel 0 : 0,89 % de "undelivered"
--     pour 2026/04/20. 2026/03/23, bonne cohorte, est a 0,88 %. 2026/02/09 a 0,82 %.
--     Un numero perime ou reattribue produirait un echec de livraison, pas ca.
--   * reactivite des familles : 23,4 % des familles de 2026/04/20 envoient au moins
--     un SMS entrant pendant la fenetre -- exactement 2026/05/04 (23,3 %) et
--     2026/08/24 (23,4 %). Les familles sont joignables, elles repondent par ecrit.
--   * duree de sonnerie (started_at -> answered_at) : 16,1 s pour 2026/04/20 contre
--     14,5 a 16,2 s partout. Aucune anomalie reseau ni de ligne appelante.
--
-- APPORT TECHNIQUE : la source MOTS_DL.MOTS_APP.AIRCALL_CALLS porte ANSWERED_AT,
-- ENDED_AT et MISSED_CALL_REASON, qui ne sont PAS exposes par
-- staging.stg_1001mots_app__aircall_calls. `answered_at` leve la limite documentee
-- de `duration` (qui inclut la sonnerie et produit une distribution bimodale) :
-- duree de conversation = datediff(second, answered_at, ended_at). C'est la bonne
-- mesure de joignabilite, a preferer a `duration` dans toute analyse nouvelle.
--
-- Avec cette mesure propre, le mecanisme d'avril est net et porte sur 100 % de la
-- population (pas seulement l'echantillon transcrit de d13) :
--   conversations >= 120 s : 23,2 % pour 2026/04/20 contre 29,0 a 38,3 % ailleurs
--   conversations >= 300 s : 17,8 % contre 24,1 a 31,8 %
--
-- Chaque bloc est autonome.

-- (1) Vraie duree de conversation et sonnerie, par cohorte
with fam as (
  select family_id, left(cohort_name,10) coh
  from fact.super_table
  where cohort_name like '2026/%' and left(cohort_name,10) between '2026/01/12' and '2026/09/07'
    and nullif(trim(call_0_status),'') is not null),
ac as (
  select a.child_support_id fid, a.answered,
         datediff(second, a.answered_at, a.ended_at) conv_s,
         datediff(second, a.started_at, a.answered_at) sonnerie_s
  from MOTS_DL.MOTS_APP.aircall_calls a
  where a.call_session = 0 and a.direction = 'outbound')
select f.coh, count(*) tentatives,
       round(100.0*count_if(a.answered)/count(*),1) pct_answered,
       round(100.0*count_if(a.conv_s >= 120)/count(*),1) pct_conv_120,
       round(100.0*count_if(a.conv_s >= 300)/count(*),1) pct_conv_300,
       round(avg(a.sonnerie_s),1) sonnerie_moy
from fam f join ac a on a.fid = f.family_id
group by 1 order by 1

;

-- (2) SMS pendant la fenetre d'appel 0 : delivrabilite et reactivite des familles
-- Statuts possibles cote sortant : delivered, sent, undelivered, not_sent.
with fam as (
  select distinct st.family_id, left(st.cohort_name,10) coh,
         g.date_call0_start::date deb, g.date_call0_end::date fin
  from fact.super_table st
  inner join dim.child c on c.child_id = st.child_id
  inner join dim.groups g on g.group_id = c.group_id
  where st.cohort_name like '2026/%' and left(st.cohort_name,10) between '2026/01/12' and '2026/09/07'
    and nullif(trim(st.call_0_status),'') is not null),
m as (
  select child_support_id fid, direction, status, created_at::date dt
  from staging.stg_1001mots_app__aircall_messages),
j as (
  select f.coh, f.family_id,
         count_if(m.direction='outbound') sortants,
         count_if(m.direction='outbound' and m.status='undelivered') echecs,
         count_if(m.direction='inbound') entrants
  from fam f left join m on m.fid = f.family_id and m.dt between f.deb and f.fin
  group by 1,2)
select coh, count(*) n_fam,
       round(avg(sortants),2) sms_sortants_moy,
       round(100.0*sum(echecs)/nullif(sum(sortants),0),2) pct_undelivered,
       round(100.0*count_if(entrants > 0)/count(*),1) pct_fam_a_repondu,
       round(avg(entrants),2) sms_entrants_moy
from j group by 1 order by 1

;

-- (3) Les familles connaissaient-elles l'association ? SMS du programme (spot-hit)
-- recus AVANT l'ouverture de la fenetre d'appel.
-- Resultat : 99,3 % des familles de 2026/04/20 en avaient recu, 4,02 en moyenne,
-- premier SMS 38,6 jours avant l'appel -- en ligne avec toutes les autres cohortes.
-- L'hypothese "l'appel etait un appel froid d'un numero inconnu" est ECARTEE.
with fam as (
  select distinct st.family_id, left(st.cohort_name,10) coh,
         ch.parent1_id, g.date_call0_start::date deb, g.date_call0_end::date fin
  from fact.super_table st
  join staging.stg_1001mots_app__children ch on ch.child_id = st.child_id
  join dim.child c on c.child_id = st.child_id
  join dim.groups g on g.group_id = c.group_id
  where st.cohort_name like '2026/%' and left(st.cohort_name,10) between '2026/01/12' and '2026/09/07'
    and nullif(trim(st.call_0_status),'') is not null),
ev as (
  select related_id::int pid, min(date_occurred) premier_sms
  from staging.stg_1001mots_app__events
  where type = 'Events::TextMessage' and spot_hit_status in ('1','2')
  group by 1),
evw as (
  select f.family_id, count_if(e.date_occurred < f.deb) sms_avant
  from staging.stg_1001mots_app__events e
  join fam f on f.parent1_id::int = e.related_id::int
  where e.type = 'Events::TextMessage' and e.spot_hit_status in ('1','2')
  group by 1)
select f.coh, count(*) n_fam,
       round(100.0*count_if(coalesce(w.sms_avant,0) > 0)/count(*),1) pct_sms_avant,
       round(avg(coalesce(w.sms_avant,0)),2) sms_avant_moy,
       round(avg(datediff(day, e.premier_sms, f.deb)),1) jours_1er_sms_avant_appel
from fam f
left join ev e on e.pid = f.parent1_id::int
left join evw w on w.family_id = f.family_id
group by 1 order by 1
