-- Granularité : 1 ligne par famille × session × niveau du funnel
-- Permet de construire un Sankey visuellement dans Metabase (visual builder)
--
-- Étapes Metabase :
--   1. Nouvelle question → Choisir la table fact.sessions_funnel
--   2. Filtres (optionnels) : cohort_name, level
--   3. Résumé : Count of rows
--   4. Group by : source, puis target
--   5. Visualisation : Sankey

with s as (
    select
        family_id,
        call_session,
        cohort_name,
        supporter_name,
        session_outcome,
        had_calendly_sms,
        has_call_status
    from {{ ref('sessions_with_booking') }}
    where session_outcome in ('RDV pris', 'RDV annulé')
),

-- Niveau 0 : Sessions avec RDV → Avec/Sans SMS Calendly
level_0 as (
    select
        family_id,
        call_session,
        cohort_name,
        supporter_name,
        0 as level,
        'Sessions avec RDV' as source,
        case when had_calendly_sms = 1 then 'Avec SMS Calendly' else 'Sans SMS Calendly' end as target
    from s
),

-- Niveau 1 : Avec/Sans SMS → RDV pris / RDV annulé
level_1 as (
    select
        family_id,
        call_session,
        cohort_name,
        supporter_name,
        1 as level,
        case when had_calendly_sms = 1 then 'Avec SMS Calendly' else 'Sans SMS Calendly' end as source,
        session_outcome as target
    from s
),

-- Niveau 2 : RDV pris/annulé → Statut connu / À venir
level_2 as (
    select
        family_id,
        call_session,
        cohort_name,
        supporter_name,
        2 as level,
        session_outcome as source,
        case when has_call_status = 1 then 'Statut connu' else 'À venir' end as target
    from s
)

select * from level_0
union all
select * from level_1
union all
select * from level_2
