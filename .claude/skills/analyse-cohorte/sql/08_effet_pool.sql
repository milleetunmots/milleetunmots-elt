-- 08 | Effet de POOL : quelle aurait ete la performance si les appels avaient ete
-- repartis sur TOUTES les accompagnantes, et qu'apporte le pool effectivement choisi ?
--
-- Barre du vivier = moyenne des accompagnantes actives a la date de la cellule
-- (>= 20 appels d'historique sur la meme session dans les 13 mois precedents),
-- ponderee par leur capacite. Effet de pool = projection du pool reel - cette barre.
--
-- Resultat (2026, call 0) : la barre du vivier tient dans 1,8 point toute l'annee
-- (64,2 a 66,0) -- la capacite moyenne de l'equipe n'a pas bouge. L'effet de pool
-- va de -2,8 a +3,1, soit 6 points d'amplitude, contre 23 points pour l'execution.
-- Le choix des accompagnantes compte environ quatre fois moins que ce qui se joue
-- pendant la cohorte. Correlation taille du pool / effet de pool : +0,81 -- les
-- cohortes confiees a peu d'accompagnantes partent en dessous de la barre.
-- PARAM : '2026/%' et call_session = 0.

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

-- Cellules évaluées : cohorte x session, avec leur date de fin
cible as (
    select cohort_name, call_session, max(session_end) as fin
    from appel_all
    where cohort_name like '2026/%'
    group by 1, 2
),

-- Volume de chaque accompagnante dans la cellule évaluée
volume as (
    select cohort_name, call_session, supporter_name,
           count(*) as n, count_if(ok = 1) as n_ok
    from appel_all
    where cohort_name like '2026/%'
    group by 1, 2, 3
),

-- Historique 13 mois antérieurs de chaque accompagnante, MÊME session,
-- en excluant la cohorte évaluée (sinon la projection se prédit elle-même)
historique as (
    select
        v.cohort_name, v.call_session, v.supporter_name, v.n, v.n_ok,
        count(h.family_id)                   as n_hist,
        count_if(h.ok = 1)                   as n_hist_ok,
        avg(h.ok)                            as hist_ok,
        avg(iff(h.ok = 1, h.pm, null))       as hist_pm
    from volume as v
    join cible as c
      on c.cohort_name = v.cohort_name and c.call_session = v.call_session
    left join appel_all as h
      on  h.supporter_name = v.supporter_name
      and h.call_session   = v.call_session
      and h.cohort_name   <> v.cohort_name
      and h.session_end    <  c.fin
      and h.session_end   >= dateadd(month, -13, c.fin)
    group by 1, 2, 3, 4, 5
),

-- Vivier : TOUTES les accompagnantes actives a la date de la cellule
-- (>= 20 appels d'historique sur la meme session dans les 13 mois precedents),
-- qu'elles aient travaille sur cette cohorte ou non.
tous_supporters as (
    select distinct supporter_name from appel_all
),
vivier as (
    select c.cohort_name, c.call_session, s.supporter_name,
           count(h.family_id) as n_hist,
           avg(h.ok)          as hist_ok
    from cible as c
    cross join tous_supporters as s
    left join appel_all as h
      on  h.supporter_name = s.supporter_name
      and h.call_session   = c.call_session
      and h.cohort_name   <> c.cohort_name
      and h.session_end    <  c.fin
      and h.session_end   >= dateadd(month, -13, c.fin)
    group by 1, 2, 3
    having count(h.family_id) >= 20
),
barres as (
    select cohort_name, call_session,
           count(*)                                          as taille_vivier,
           100.0*avg(hist_ok)                                as ref_equiponderee,
           100.0*sum(n_hist*hist_ok)/sum(n_hist)             as ref_capacite
    from vivier group by 1, 2
),
pool_reel as (
    select cohort_name, call_session, count(*) as taille_pool, sum(n) as n,
           100.0*sum(iff(n_hist>0, n*hist_ok, 0))/nullif(sum(iff(n_hist>0,n,0)),0) as proj_pool
    from historique group by 1, 2
),
reel as (
    select cohort_name, call_session, 100.0*avg(ok) as reel_ok
    from appel_all where cohort_name like '2026/%' group by 1, 2
)
select left(b.cohort_name,10) coh,
       b.taille_vivier, p.taille_pool,
       round(b.ref_capacite,1)               ref_toutes,
       round(p.proj_pool,1)                  proj_pool,
       round(p.proj_pool-b.ref_capacite,1)   effet_pool,
       round(r.reel_ok,1)                    reel,
       round(r.reel_ok-p.proj_pool,1)        effet_execution
from barres b
join pool_reel p on p.cohort_name=b.cohort_name and p.call_session=b.call_session
join reel     r on r.cohort_name=b.cohort_name and r.call_session=b.call_session
where b.call_session = 0
order by 1
