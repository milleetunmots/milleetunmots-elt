-- 02 | Projete vs realise par cohorte x session.
-- Projection = performance des accompagnantes de la cellule sur les 13 mois PRECEDANT
-- la fin de la session, meme session, cohorte evaluee exclue, ponderee par leur volume.
-- PARAM : remplacer '2026/%' (3 occurrences) par la cohorte ou l'annee voulue.
-- Fenetre : dateadd(month, -13, c.fin) -- passer a -6 pour coller a la tendance recente.
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

-- Projection = moyenne des taux historiques pondérée par le volume de la cellule
projection as (
    select
        cohort_name, call_session,
        count(*) as nb_accompagnantes,
        sum(n)   as nb_appels,
        100.0 * sum(iff(n_hist > 0, n * hist_ok, 0))
              / nullif(sum(iff(n_hist > 0, n, 0)), 0)                as proj_ok,
        100.0 * sum(iff(n_hist_ok > 0, n_ok * hist_pm, 0))
              / nullif(sum(iff(n_hist_ok > 0, n_ok, 0)), 0)          as proj_pm,
        round(100.0 * sum(iff(n_hist = 0, n, 0)) / sum(n), 1)        as pct_appels_sans_historique,
        min(n_hist)                                                  as min_appels_historique
    from historique
    group by 1, 2
),

realise as (
    select cohort_name, call_session,
           100.0 * avg(ok)                        as reel_ok,
           100.0 * avg(iff(ok = 1, pm, null))     as reel_pm
    from appel_all
    where cohort_name like '2026/%'
    group by 1, 2
)

select
    left(p.cohort_name, 10)              as date_cohorte,
    p.cohort_name                        as cohorte,
    p.call_session                       as session_appel,
    p.nb_accompagnantes,
    p.nb_appels,
    round(p.proj_ok, 1)                  as projete_appels_ok,
    round(r.reel_ok, 1)                  as reel_appels_ok,
    round(r.reel_ok - p.proj_ok, 1)      as ecart_appels_ok,
    round(p.proj_pm, 1)                  as projete_pm,
    round(r.reel_pm, 1)                  as reel_pm,
    round(r.reel_pm - p.proj_pm, 1)      as ecart_pm,
    p.pct_appels_sans_historique,
    p.min_appels_historique
from projection as p
join realise as r
  on r.cohort_name = p.cohort_name and r.call_session = p.call_session
order by 1, 3
