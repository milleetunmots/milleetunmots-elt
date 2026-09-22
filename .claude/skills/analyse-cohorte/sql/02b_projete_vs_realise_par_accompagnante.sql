-- 02b | Projete vs realise PAR ACCOMPAGNANTE sur un perimetre de cohortes.
-- Chaque accompagnante est comparee a SA propre projection : qui a change, et de combien.
-- Lire ecart_* (la derive) ET reel_* (le niveau) ensemble : ce n'est pas la meme question.
-- PARAM : remplacer '2026/%' (4 occurrences), seuil de volume : nb_appels >= 100.
-- Projeté vs réalisé PAR ACCOMPAGNANTE (cohortes 2026)
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

-- Agrégation au niveau accompagnante : sa projection vs son réalisé
par_accompagnante as (
    select
        supporter_name,
        count(*)      as nb_cellules,
        sum(n)        as nb_appels,
        sum(n_ok)     as nb_appels_ok,
        sum(n_hist)   as nb_appels_historique,
        100.0 * sum(iff(n_hist > 0, n * hist_ok, 0))
              / nullif(sum(iff(n_hist > 0, n, 0)), 0)              as proj_ok,
        100.0 * sum(iff(n_hist_ok > 0, n_ok * hist_pm, 0))
              / nullif(sum(iff(n_hist_ok > 0, n_ok, 0)), 0)        as proj_pm
    from historique
    group by 1
),

realise as (
    select supporter_name,
           100.0 * avg(ok)                      as reel_ok,
           100.0 * avg(iff(ok = 1, pm, null))   as reel_pm
    from appel_all
    where cohort_name like '2026/%'
    group by 1
)

select
    split_part(p.supporter_name, '@', 1)     as accompagnante,
    p.nb_appels,
    p.nb_appels_ok,
    p.nb_appels_historique,

    round(p.proj_ok, 1)                      as projete_appels_ok,
    round(r.reel_ok, 1)                      as reel_appels_ok,
    round(r.reel_ok - p.proj_ok, 1)          as ecart_appels_ok,
    round(p.nb_appels * (r.reel_ok - p.proj_ok) / 100, 0)    as ecart_ok_en_appels,

    round(p.proj_pm, 1)                      as projete_pm,
    round(r.reel_pm, 1)                      as reel_pm,
    round(r.reel_pm - p.proj_pm, 1)          as ecart_pm,
    round(p.nb_appels_ok * (r.reel_pm - p.proj_pm) / 100, 0)  as ecart_pm_en_missions

from par_accompagnante as p
join realise as r on r.supporter_name = p.supporter_name
where p.nb_appels >= 100          -- seuil de volume, sinon l'écart est du bruit
order by ecart_appels_ok
