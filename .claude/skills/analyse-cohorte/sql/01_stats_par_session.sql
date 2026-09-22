-- 01 | Taux d'appels OK, de petites missions et couverture, par cohorte x session.
-- Point de depart de toute analyse : niveau absolu et reperage des cellules anormales.
-- PARAM : remplacer '2026/%' (2 occurrences) par la cohorte ou l'annee voulue.
-- Appels OK et petites missions par cohorte 2026 x session
-- Denominateur : appels ayant un statut renseigne
with base as (
    select
        st.family_id, st.child_id, st.cohort_name,
        st.call_0_status, st.call1_status, st.call2_status, st.call3_status,
        st.is_call0_goals, st.is_call1_goals, st.is_call2_goals, st.is_call3_goals,
        g.date_call0_end::date as d0, g.date_call1_end::date as d1,
        g.date_call2_end::date as d2, g.date_call3_end::date as d3
    from fact.super_table as st
    inner join dim.child  as c on c.child_id = st.child_id
    inner join dim.groups as g on g.group_id = c.group_id
),
sessions as (
    select family_id, cohort_name, 0 as call_session, call_0_status as call_status, is_call0_goals as is_goals, d0 as session_end from base
    union all select family_id, cohort_name, 1, call1_status, is_call1_goals, d1 from base
    union all select family_id, cohort_name, 2, call2_status, is_call2_goals, d2 from base
    union all select family_id, cohort_name, 3, call3_status, is_call3_goals, d3 from base
),
famille_session as (
    select family_id, cohort_name, call_session,
           max(call_status) as call_status,
           max(is_goals)    as is_goals,
           max(session_end) as session_end,
           iff(nullif(trim(max(call_status)),'') is not null, 1, 0) as a_statut
    from sessions group by 1,2,3
),
tous as (
    select cohort_name, call_session, count(*) as nb_familles,
           count_if(a_statut=1) as nb_traites
    from famille_session
    where session_end <= current_date and cohort_name like '2026/%'
    group by 1,2
)
select left(f.cohort_name,10) as coh, f.call_session as s,
       t.nb_familles, t.nb_traites,
       round(100.0*t.nb_traites/t.nb_familles,1) as pct_couverture,
       round(100.0*count_if(f.call_status='OK')/count(*),1) as pct_ok,
       round(100.0*count_if(f.call_status='OK' and f.is_goals=1)/nullif(count_if(f.call_status='OK'),0),1) as pct_pm
from famille_session f
join tous t on t.cohort_name=f.cohort_name and t.call_session=f.call_session
where f.session_end <= current_date and f.cohort_name like '2026/%' and f.a_statut = 1
group by 1,2,3,4,5
order by 1,2
