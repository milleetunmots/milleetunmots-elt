-- d02 | Ecarter (ou non) l'hypothese calendaire : vacances, ponts, periode creuse.
-- Liste les cellules par DATE DE FIN de session. Deux cellules qui partagent la
-- meme fenetre ont ete appelees les memes semaines par les memes accompagnantes.
-- Resultat (avril 2026) : au 3 mai, 2026/03/23 S1 faisait +1,9 pendant que
-- 2026/04/20 S0 faisait -15,7. Au 19 avril, 2026/03/09 S1 +3,0 contre
-- 2026/04/06 S0 -7,4. Hypothese calendaire morte.
-- PARAM : la fenetre de dates dans le where final.

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
