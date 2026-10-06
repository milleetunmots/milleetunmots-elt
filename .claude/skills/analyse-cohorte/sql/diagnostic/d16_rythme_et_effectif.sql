-- d16 | L'execution dans la fenetre : rythme d'appel par famille et effectif de
-- l'equipe jour par jour. Complete d04 (charge de travail), qui mesurait le VOLUME
-- a passer ; ici on mesure la MANIERE de le passer et la force de frappe disponible.
--
-- VERDICT (2026-10-02) : ECARTEE sur les deux niveaux.
--   * rythme par famille, 2026/04/20 : 2,80 tentatives etalees sur 2,12 jours
--     distincts, premier appel 1,2 jour apres l'ouverture de la fenetre. 2026/02/09
--     (2,84 / 2,19), 2026/03/09 (2,88 / 2,30) et 2026/03/23 (2,78 / 2,19) ont le meme
--     profil et vont bien. Pas de demarrage tardif, pas de tentatives tassees sur un
--     seul jour, et plutot PLUS d'effort que la moyenne.
--   * effectif reellement actif, fenetre 20/04 -> 03/05 : 20 a 25 accompagnantes par
--     jour ouvre pour 163 a 290 appels, soit 7,0 a 11,6 appels par personne. Identique
--     aux quinzaines qui precedent et qui suivent. L'hypothese "l'equipe etait en
--     partie absente pendant les vacances de printemps, donc incapable d'honorer les
--     rappels convenus" est ECARTEE -- elle etait seduisante parce qu'elle expliquait
--     a la fois l'uniformite du trou et sa disparition en session 1.
--
-- Chaque bloc est autonome.

-- (1) Rythme d'appel par famille sur la fenetre de 13 jours
with fam as (
  select st.family_id, left(st.cohort_name,10) coh,
         g.date_call0_start::date deb
  from fact.super_table st
  inner join dim.child c on c.child_id = st.child_id
  inner join dim.groups g on g.group_id = c.group_id
  where st.cohort_name like '2026/%' and left(st.cohort_name,10) between '2026/01/12' and '2026/09/07'
    and nullif(trim(st.call_0_status),'') is not null),
f as (select distinct family_id, coh, deb from fam),
ac as (
  select child_support_id fid, started_at, date_started
  from staging.stg_1001mots_app__aircall_calls
  where call_session = 0 and direction = 'outbound'),
j as (
  select f.coh, f.family_id,
         count(a.started_at) tentatives,
         count(distinct a.date_started) jours_distincts,
         datediff(day, f.deb, min(a.date_started)) jour_1er_appel,
         datediff(day, min(a.date_started), max(a.date_started)) amplitude
  from f left join ac a on a.fid = f.family_id
  group by 1,2,f.deb)
select coh, count(*) n_fam,
       round(avg(tentatives),2) tentatives_moy,
       round(avg(jours_distincts),2) jours_moy,
       round(avg(jour_1er_appel),1) delai_demarrage,
       round(avg(amplitude),1) amplitude_moy
from j where tentatives > 0
group by 1 order by 1

;

-- (2) Effectif actif par jour. PARAM : la plage de dates.
-- /!\ caller_id est l'identifiant de l'agent Aircall, PAS un numero de ligne
-- sortante. La source ne porte aucun numero appelant, donc l'hypothese d'une ligne
-- flaggee comme spam n'est pas testable par cette colonne -- elle l'est
-- indirectement par la duree de sonnerie (d14, bloc 1), qui est normale.
select date_started jour, dayname(date_started) jsem,
       count(distinct caller_id) nb_accompagnantes_actives,
       count(*) appels,
       round(count(*)/nullif(count(distinct caller_id),0),1) appels_par_personne,
       round(100.0*count_if(duration >= 300)/count(*),1) pct_conv_longue
from staging.stg_1001mots_app__aircall_calls
where direction = 'outbound' and date_started between '2026-04-06' and '2026-05-17'
group by 1,2 order by 1
