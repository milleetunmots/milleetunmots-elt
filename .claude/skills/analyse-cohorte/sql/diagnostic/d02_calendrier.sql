-- d02 | Ecarter (ou non) l'hypothese calendaire : vacances, ponts, periode creuse.
-- Liste les cellules par DATE DE FIN de session. Deux cellules qui partagent la
-- meme fenetre ont ete appelees les memes semaines par les memes accompagnantes.
-- Resultat (avril 2026) : au 3 mai, 2026/03/23 S1 faisait +1,9 pendant que
-- 2026/04/20 S0 faisait -15,7. Au 19 avril, 2026/03/09 S1 +3,0 contre
-- 2026/04/06 S0 -7,4. Hypothese calendaire morte.
-- PARAM : la fenetre de dates dans le where final.
--
-- /!\ FAIBLESSE DE CE TEST, et comment la corriger (2026-10-02).
-- Les cellules jumelles qu'il compare ne sont pas du meme RANG de session : il
-- oppose 2026/04/20 S0, un premier contact depuis un numero inconnu, a 2026/03/23 S1,
-- un appel chaud ou la famille connait son accompagnante et a son numero. Une periode
-- creuse pourrait peser sur l'un sans toucher l'autre. "Memes semaines, resultats
-- opposes" ne suffit donc pas.
-- Deux blocs ont ete ajoutes en bas du fichier pour y repondre. Verdict inchange,
-- l'hypothese calendaire reste MORTE, mais maintenant correctement :
--   * bloc (2) : ecart de chaque cellule a la norme de SA session, sur la vraie duree
--     de conversation. Dans chaque quinzaine, la seule cellule en dessous est celle
--     d'une cohorte d'avril -- 2026/04/20 S0 a -9,0 quand 2026/03/23 S1 est a +0,8 et
--     2026/02/23 S2 a +0,8 les memes jours. Et dans la fenetre suivante, 2026/04/06 S1
--     est a -7,6 quand 2026/05/04 S0, un PREMIER CONTACT en plein pont du 1er et du
--     8 mai, est a -0,3. Le deficit suit la cohorte, pas le calendrier.
--   * bloc (3) : indice de periode pur, hors cohortes d'avril. Les deux semaines de la
--     fenetre de 2026/04/20 pesent -0,0 point en moyenne ponderee (+1,1 et -1,8), dans
--     une serie hebdomadaire qui va de -6,3 a +3,9 sur le printemps. Aucun effet.
--
-- A noter pour qui refait un test apparie accompagnante x jour : comparer des taux
-- BRUTS entre rangs de session est une erreur. Une session 0 convertit normalement
-- mieux (norme 32,0 % de conversations >= 2 min) qu'une S1 (28,1) ou une S2 (26,1).
-- Sans ramener chaque cote a sa norme, l'appariement donne -0,1 point et conclut a
-- tort a l'absence d'effet cohorte ; avec la correction il donne -4,9.

-- Projeté vs réalisé par cohorte x session
-- Projection = performance des accompagnantes de la cellule sur les 13 mois
-- PRÉCÉDANT la fin de la session, même session, cohorte évaluée exclue.
with base as (
    select
        st.family_id, st.child_id, st.cohort_name, st.supporter_name,
        st.call_0_status, st.call1_status, st.call2_status, st.call3_status,
        st.is_call0_goals, st.is_call1_goals, st.is_call2_goals, st.is_call3_goals,
        g.date_call0_end::date as d0, g.date_call1_end::date as d1,
        g.date_call2_end::date as d2, g.date_call3_end::date as d3
    from fact.super_table as st
    inner join dim.child  as c on c.child_id = st.child_id
    inner join dim.groups as g on g.group_id = c.group_id
),

sessions as (
    select family_id, cohort_name, supporter_name, 0 as call_session,
           call_0_status as call_status, is_call0_goals as is_goals, d0 as session_end from base
    union all
    select family_id, cohort_name, supporter_name, 1, call1_status, is_call1_goals, d1 from base
    union all
    select family_id, cohort_name, supporter_name, 2, call2_status, is_call2_goals, d2 from base
    union all
    select family_id, cohort_name, supporter_name, 3, call3_status, is_call3_goals, d3 from base
),

-- 1 ligne par famille x session (les fratries partagent le même appel)
appel as (
    select
        family_id, cohort_name, supporter_name, call_session,
        max(session_end) as session_end,
        iff(max(call_status) = 'OK', 1, 0)                           as ok,
        iff(nullif(trim(max(call_status)),'') is not null, 1, 0)     as a_statut,
        iff(max(call_status) = 'OK' and max(is_goals) = 1, 1, 0)     as pm
    from sessions
    group by 1, 2, 3, 4
),

-- Univers complet (toutes années) : sert d'historique
appel_all as (
    select * from appel
    where session_end is not null
      and session_end <= current_date
      and a_statut = 1          -- <<< denominateur : appels ayant un statut
),

-- Toutes les cellules 2025 + 2026 (pas seulement 2026)
cible as (
    select cohort_name, call_session, max(session_end) as fin
    from appel_all
    where left(cohort_name,4) in ('2025','2026')
    group by 1, 2
),
volume as (
    select cohort_name, call_session, supporter_name,
           count(*) as n, count_if(ok=1) as n_ok
    from appel_all where left(cohort_name,4) in ('2025','2026') group by 1,2,3
),
historique as (
    select v.cohort_name, v.call_session, v.supporter_name, v.n, v.n_ok,
           count(h.family_id) as n_hist, count_if(h.ok=1) as n_hist_ok,
           avg(h.ok) as hist_ok, avg(iff(h.ok=1,h.pm,null)) as hist_pm
    from volume v
    join cible c on c.cohort_name=v.cohort_name and c.call_session=v.call_session
    left join appel_all h
      on h.supporter_name=v.supporter_name and h.call_session=v.call_session
     and h.cohort_name<>v.cohort_name
     and h.session_end < c.fin and h.session_end >= dateadd(month,-13,c.fin)
    group by 1,2,3,4,5
),
proj as (
    select cohort_name, call_session, sum(n) as n,
           100.0*sum(iff(n_hist>0,n*hist_ok,0))/nullif(sum(iff(n_hist>0,n,0)),0) as proj_ok
    from historique group by 1,2
),
reel as (
    select cohort_name, call_session, max(session_end) as fin, 100.0*avg(ok) as reel_ok
    from appel_all where left(cohort_name,4) in ('2025','2026') group by 1,2
)
select left(p.cohort_name,10) as coh, p.call_session as s,
       r.fin as fin_session, p.n as appels,
       round(p.proj_ok,1) as proj, round(r.reel_ok,1) as reel,
       round(r.reel_ok-p.proj_ok,1) as ecart
from proj p join reel r on r.cohort_name=p.cohort_name and r.call_session=p.call_session
where p.n >= 50 and r.fin between '2026-03-15' and '2026-06-15'
order by r.fin

;

-- (2) Ecart de chaque cellule a la norme de SA session, sur la duree de conversation
-- reelle (answered_at -> ended_at, cf. d14). Trier par `deb` et lire par groupes de
-- cellules qui partagent la meme fenetre.
with w as (
  select st.family_id, left(st.cohort_name,10) coh,
         g.date_call0_start::date s0, g.date_call1_start::date s1,
         g.date_call2_start::date s2, g.date_call3_start::date s3,
         g.date_call0_end::date e0, g.date_call1_end::date e1,
         g.date_call2_end::date e2, g.date_call3_end::date e3
  from fact.super_table st
  inner join dim.child c on c.child_id = st.child_id
  inner join dim.groups g on g.group_id = c.group_id
  where st.cohort_name like '2026/%'),
win as (
  select distinct family_id, coh, 0 cs, s0 deb, e0 fin from w
  union all select distinct family_id, coh, 1, s1, e1 from w
  union all select distinct family_id, coh, 2, s2, e2 from w
  union all select distinct family_id, coh, 3, s3, e3 from w),
ac as (
  select a.child_support_id fid, a.call_session cs,
         iff(datediff(second, a.answered_at, a.ended_at) >= 120, 1, 0) conv
  from MOTS_DL.MOTS_APP.aircall_calls a
  where a.direction = 'outbound'),
cell as (
  select win.coh, win.cs, max(win.deb) deb, count(*) tentatives, avg(a.conv) r
  from win join ac a on a.fid = win.family_id and a.cs = win.cs
  where win.fin <= current_date
  group by 1,2
  having tentatives >= 100)
select coh, cs, deb, tentatives,
       round(100.0*r,1) conv_120,
       round(100.0*(r - avg(r) over (partition by cs)),1) ecart_norme_session
from cell
order by deb, cs

;

-- (3) Indice de periode pur, semaine par semaine, cohortes d'avril EXCLUES
-- (sinon elles fabriquent l'effet qu'on cherche a mesurer).
with fam as (
  select distinct family_id, left(cohort_name,10) coh from fact.super_table
  where cohort_name like '2026/%'),
ac as (
  select a.started_at::date jour, a.child_support_id fid, a.call_session cs,
         iff(datediff(second, a.answered_at, a.ended_at) >= 120, 1, 0) conv
  from MOTS_DL.MOTS_APP.aircall_calls a
  where a.direction = 'outbound' and a.started_at >= '2026-01-01'),
hors_avril as (
  select a.*, f.coh from ac a join fam f on f.family_id = a.fid
  where f.coh not in ('2026/04/06','2026/04/20')),
norme as (select cs, avg(conv) r from hors_avril group by 1)
select date_trunc('week', h.jour)::date semaine,
       count(*) appels,
       round(100.0*avg(h.conv),1) conv_120,
       round(100.0*avg(h.conv - n.r),1) ecart_norme_session
from hors_avril h join norme n on n.cs = h.cs
where h.jour between '2026-03-09' and '2026-06-14'
group by 1 order by 1
