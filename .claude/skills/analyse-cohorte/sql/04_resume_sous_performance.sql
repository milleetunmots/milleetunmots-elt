-- 04 | Resume par cohorte : combien d'accompagnantes sous leur projection,
-- et quelle part des appels de la cohorte elles portent.
-- La colonne qui compte est pct_appels_sous_5pts_*, pas le nombre d'accompagnantes :
-- environ la moitie est mecaniquement sous projection a -0,5 pt, ce qui ne veut rien dire.
-- PARAM : remplacer '2026/%' (4 occurrences) ; seuil -5 pts (4 occurrences).
-- Résumé par cohorte : combien d'accompagnantes sous leur projection,
-- et quelle part des appels de la cohorte elles portent
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

-- Réalisé de chaque accompagnante dans chaque cellule
reel_cellule as (
    select cohort_name, call_session, supporter_name,
           count(*)                            as n_reel,
           count_if(ok = 1)                    as n_ok_reel,
           100.0 * avg(ok)                     as reel_ok,
           100.0 * avg(iff(ok = 1, pm, null))  as reel_pm
    from appel_all
    where cohort_name like '2026/%'
    group by 1, 2, 3
),

-- Consolidation au niveau accompagnante x cohorte (les 4 sessions cumulées)
accompagnante_cohorte as (
    select
        h.cohort_name,
        h.supporter_name,
        sum(h.n)     as nb_appels,
        sum(h.n_ok)  as nb_appels_ok,
        100.0 * sum(h.n * h.hist_ok)
              / nullif(sum(iff(h.n_hist > 0, h.n, 0)), 0)            as projete_ok,
        100.0 * sum(h.n_ok * h.hist_pm)
              / nullif(sum(iff(h.n_hist_ok > 0, h.n_ok, 0)), 0)      as projete_pm,
        sum(r.n_reel    * r.reel_ok) / nullif(sum(r.n_reel), 0)      as reel_ok,
        sum(r.n_ok_reel * r.reel_pm) / nullif(sum(r.n_ok_reel), 0)   as reel_pm
    from historique   as h
    join reel_cellule as r
      on r.cohort_name    = h.cohort_name
     and r.call_session   = h.call_session
     and r.supporter_name = h.supporter_name
    group by 1, 2
),

ecarts as (
    select *,
        reel_ok - projete_ok                                   as ecart_ok,
        reel_pm - projete_pm                                   as ecart_pm,
        sum(nb_appels)    over (partition by cohort_name)       as nb_appels_cohorte,
        sum(nb_appels_ok) over (partition by cohort_name)       as nb_appels_ok_cohorte
    from accompagnante_cohorte
)

select
    left(cohort_name, 10)                                              as date_cohorte,
    max(nb_appels_cohorte)                                             as nb_appels,
    count(*)                                                           as nb_accompagnantes,

    -- APPELS OK
    round(sum(nb_appels * ecart_ok) / max(nb_appels_cohorte), 1)       as ecart_cohorte_ok,
    count_if(ecart_ok < 0)                                             as nb_sous_projection_ok,
    round(100.0 * sum(iff(ecart_ok < 0, nb_appels, 0))
                / max(nb_appels_cohorte), 0)                           as pct_appels_sous_projection_ok,
    count_if(ecart_ok < -5)                                            as nb_sous_5pts_ok,
    round(100.0 * sum(iff(ecart_ok < -5, nb_appels, 0))
                / max(nb_appels_cohorte), 0)                           as pct_appels_sous_5pts_ok,

    -- PETITES MISSIONS
    round(sum(nb_appels_ok * ecart_pm) / max(nb_appels_ok_cohorte), 1) as ecart_cohorte_pm,
    count_if(ecart_pm < 0)                                             as nb_sous_projection_pm,
    round(100.0 * sum(iff(ecart_pm < 0, nb_appels_ok, 0))
                / max(nb_appels_ok_cohorte), 0)                        as pct_appels_ok_sous_projection_pm,
    count_if(ecart_pm < -5)                                            as nb_sous_5pts_pm,
    round(100.0 * sum(iff(ecart_pm < -5, nb_appels_ok, 0))
                / max(nb_appels_ok_cohorte), 0)                        as pct_appels_ok_sous_5pts_pm

from ecarts
group by 1
order by 1
