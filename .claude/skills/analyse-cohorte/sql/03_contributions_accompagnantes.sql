-- 03 | Decomposition de l'ecart d'une cellule en contributions par accompagnante.
-- contribution = part du volume x ecart individuel ; leur somme = l'ecart de la cellule.
-- Repond a "qui pese" et "est-ce reparti entre les accompagnantes ou tres localise".
-- Filtree sur une cellule et triee par contribution_ok : waterfall des contributeurs.
-- Agregee par cohorte x session : tableau reparti/localise.
-- PARAM : remplacer '2026/%' (4 occurrences).
-- Décomposition de l'écart projeté/réalisé d'une cohorte x session
-- en contributions par accompagnante (la somme des contributions = l'écart de la cellule)
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

-- Réalisé de chaque accompagnante DANS la cellule évaluée
reel_cellule as (
    select cohort_name, call_session, supporter_name,
           100.0 * avg(ok)                      as reel_ok,
           100.0 * avg(iff(ok = 1, pm, null))   as reel_pm
    from appel_all
    where cohort_name like '2026/%'
    group by 1, 2, 3
),

total_cellule as (
    select cohort_name, call_session,
           sum(n) as n_cellule, sum(n_ok) as n_ok_cellule
    from historique
    group by 1, 2
),

-- Contribution = part du volume x écart individuel.
-- Somme des contributions d'une cellule = écart de la cellule, exactement.
contribution as (
    select
        h.cohort_name, h.call_session, h.supporter_name,
        h.n, h.n_ok, t.n_cellule, t.n_ok_cellule,
        100.0 * h.hist_ok  as proj_ok,  r.reel_ok,
        100.0 * h.hist_pm  as proj_pm,  r.reel_pm,
        h.n    / t.n_cellule    * (r.reel_ok - 100.0 * h.hist_ok)  as contrib_ok,
        h.n_ok / t.n_ok_cellule * (r.reel_pm - 100.0 * h.hist_pm)  as contrib_pm
    from historique   as h
    join reel_cellule as r
      on r.cohort_name = h.cohort_name and r.call_session = h.call_session
     and r.supporter_name = h.supporter_name
    join total_cellule as t
      on t.cohort_name = h.cohort_name and t.call_session = h.call_session
),

rangs as (
    select *,
        row_number() over (partition by cohort_name, call_session order by contrib_ok asc)  as rang_baisse,
        row_number() over (partition by cohort_name, call_session order by contrib_ok desc) as rang_hausse
    from contribution
)

select
    left(cohort_name, 10)                        as date_cohorte,
    call_session                                 as session_appel,
    split_part(supporter_name, '@', 1)           as accompagnante,
    n                                            as nb_appels,
    round(100.0 * n / n_cellule, 1)              as part_du_volume,

    round(proj_ok, 1)                            as projete_ok,
    round(reel_ok, 1)                            as reel_ok,
    round(reel_ok - proj_ok, 1)                  as ecart_individuel_ok,
    round(contrib_ok, 2)                         as contribution_ok,
    round(contrib_pm, 2)                         as contribution_pm,
    rang_baisse,

    -- Contexte de la cellule (identique sur toutes ses lignes)
    round(sum(contrib_ok) over (partition by cohort_name, call_session), 1)  as ecart_cellule_ok,
    count(*)        over (partition by cohort_name, call_session)            as nb_accompagnantes,
    count_if(contrib_ok < 0) over (partition by cohort_name, call_session)   as nb_en_baisse,

    -- Concentration : part des 3 plus gros contributeurs dans leur camp
    round(100.0 * sum(iff(rang_baisse <= 3 and contrib_ok < 0, contrib_ok, 0))
                       over (partition by cohort_name, call_session)
              / nullif(sum(iff(contrib_ok < 0, contrib_ok, 0))
                       over (partition by cohort_name, call_session), 0), 0) as part_top3_baisse,
    round(100.0 * sum(iff(rang_hausse <= 3 and contrib_ok > 0, contrib_ok, 0))
                       over (partition by cohort_name, call_session)
              / nullif(sum(iff(contrib_ok > 0, contrib_ok, 0))
                       over (partition by cohort_name, call_session), 0), 0) as part_top3_hausse

from rangs
order by date_cohorte, session_appel, contribution_ok
