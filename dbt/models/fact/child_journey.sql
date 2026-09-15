-- Granularité : 1 ligne par transition d'état dans le parcours d'un enfant pour une cohorte
-- Permet de construire un Sankey du cycle de vie via sankey_source → sankey_target
--
-- États possibles (dans state_display) :
--   - "Début"      : nœud de départ (avant le 1er événement)
--   - "En attente" : group_status = waiting
--   - "Actif"      : 1ère activation
--   - "Réengagé"   : retour à l'état actif après désengagement (numéroté #N si plusieurs)
--   - "Call 0/1/2/3" : émis à date_callN_start si l'enfant était actif à ce moment-là
--   - "Désengagé"  : group_status in (disengaged, stopped), numéroté #N si plusieurs cycles
--   - "Fin"        : à date_call3_end si la cohorte est terminée et l'enfant encore actif
--
-- Colonne `disengagement_type` (auto/manuel) disponible pour filtrer dans Metabase.
-- Sources : super_table (périmètre), child_life_status (timeline), groups (dates de session)

with st as (
    select distinct child_id, cohort_name
    from {{ ref('super_table') }}
    where cohort_name is not null
),

cls as (
    select * from {{ ref('child_life_status') }}
),

g as (
    select * from {{ ref('groups') }}
),

-- Périmètre : statuts des enfants × cohortes de super_table uniquement
cls_scoped as (
    select cls.*
    from cls
    inner join st
        on cls.item_id     = st.child_id
        and cls.cohort_name = st.cohort_name
),

-- Événements de changement de statut bruts
-- (paused dropped : crée trop de cycles et peu informatif pour le cycle de vie)
status_events_raw as (
    select
        item_id          as child_id,
        cohort_id,
        cohort_name,
        date_from        as event_date,
        case
            when group_status = 'waiting'                    then 'En attente'
            when group_status = 'active'                     then 'Actif'
            when group_status in ('disengaged', 'stopped')   then 'Désengagé'
        end              as state,
        case
            when group_status = 'disengaged' then 'auto'
            when group_status = 'stopped'    then 'manuel'
        end              as disengagement_type,
        1                as event_priority
    from cls_scoped
    where group_status in ('waiting', 'active', 'disengaged', 'stopped')
),

-- Dédoublonner les statuts consécutifs identiques
-- (ex: 2 events 'active' à la suite suite à un changement de cohort_id → on garde le 1er)
status_events as (
    select child_id, cohort_id, cohort_name, event_date, state, disengagement_type, event_priority
    from (
        select
            *,
            lag(state) over (
                partition by child_id, cohort_id
                order by event_date
            ) as prev_status_state
        from status_events_raw
    )
    where state is distinct from prev_status_state
),

-- Événements "Call N" : émis à date_callN_start si l'enfant était actif à ce moment-là
call_session_events as (
    select
        cls_scoped.item_id   as child_id,
        cls_scoped.cohort_id,
        cls_scoped.cohort_name,
        g.date_call0_start   as event_date,
        'Call 0'             as state,
        null                 as disengagement_type,
        2                    as event_priority
    from cls_scoped
    inner join g on g.group_id = cls_scoped.cohort_id
    where g.date_call0_start is not null
      and cls_scoped.group_status = 'active'
      and g.date_call0_start >= cls_scoped.date_from
      and g.date_call0_start <  cls_scoped.date_to

    union all

    select
        cls_scoped.item_id, cls_scoped.cohort_id, cls_scoped.cohort_name,
        g.date_call1_start, 'Call 1', null, 2
    from cls_scoped
    inner join g on g.group_id = cls_scoped.cohort_id
    where g.date_call1_start is not null
      and cls_scoped.group_status = 'active'
      and g.date_call1_start >= cls_scoped.date_from
      and g.date_call1_start <  cls_scoped.date_to

    union all

    select
        cls_scoped.item_id, cls_scoped.cohort_id, cls_scoped.cohort_name,
        g.date_call2_start, 'Call 2', null, 2
    from cls_scoped
    inner join g on g.group_id = cls_scoped.cohort_id
    where g.date_call2_start is not null
      and cls_scoped.group_status = 'active'
      and g.date_call2_start >= cls_scoped.date_from
      and g.date_call2_start <  cls_scoped.date_to

    union all

    select
        cls_scoped.item_id, cls_scoped.cohort_id, cls_scoped.cohort_name,
        g.date_call3_start, 'Call 3', null, 2
    from cls_scoped
    inner join g on g.group_id = cls_scoped.cohort_id
    where g.date_call3_start is not null
      and cls_scoped.group_status = 'active'
      and g.date_call3_start >= cls_scoped.date_from
      and g.date_call3_start <  cls_scoped.date_to
),

-- Événement "Fin" : à date_call3_end si la cohorte est terminée et l'enfant encore actif
end_events as (
    select
        cls_scoped.item_id   as child_id,
        cls_scoped.cohort_id,
        cls_scoped.cohort_name,
        g.date_call3_end     as event_date,
        'Fin'                as state,
        null                 as disengagement_type,
        3                    as event_priority
    from cls_scoped
    inner join g on g.group_id = cls_scoped.cohort_id
    where g.date_call3_end is not null
      and g.date_call3_end < current_date
      and cls_scoped.group_status = 'active'
      and g.date_call3_end >= cls_scoped.date_from
      and g.date_call3_end <  cls_scoped.date_to
),

all_events as (
    select * from status_events
    union all
    select * from call_session_events
    union all
    select * from end_events
),

-- Limite stricte : aucun événement après date_call3_end (la cohorte est terminée, "Fin" est terminal)
-- Phase : nombre de Calls déjà démarrés à la date de l'événement.
-- Sert à étiqueter Désengagé/Réengagé par phase (évite les cycles Désengagé ↔ Call X).
all_events_filtered as (
    select
        e.*,
        case
            when g.date_call0_start is null or e.event_date <  g.date_call0_start then 0
            when g.date_call1_start is null or e.event_date <  g.date_call1_start then 1
            when g.date_call2_start is null or e.event_date <  g.date_call2_start then 2
            when g.date_call3_start is null or e.event_date <  g.date_call3_start then 3
            else 4
        end as phase
    from all_events as e
    inner join g on g.group_id = e.cohort_id
    where g.date_call3_end is null
       or e.event_date <= g.date_call3_end
),

ordered_events as (
    select
        *,
        row_number() over (
            partition by child_id, cohort_id
            order by event_date, event_priority
        ) as step_number
    from all_events_filtered
),

-- "Actif" est conservé uniquement pour la 1ère activation qui ne suit pas un Désengagé.
-- Sinon → "Réengagé" (couvre actif après désengagement OU activations multiples).
-- (garantit que "Actif" n'a jamais de back-edge dans le Sankey agrégé)
with_context as (
    select
        *,
        row_number() over (
            partition by child_id, cohort_id, state
            order by step_number
        ) as actif_occurrence,
        lag(state) over (
            partition by child_id, cohort_id
            order by step_number
        ) as prev_raw_state
    from ordered_events
),

relabeled_events as (
    select
        child_id,
        cohort_id,
        cohort_name,
        step_number,
        event_date,
        phase,
        disengagement_type,
        -- Étape 1 : Actif → Réengagé si pas la 1ère activation ou suit un Désengagé
        case
            when state = 'Actif'
             and (actif_occurrence > 1 or prev_raw_state = 'Désengagé')
                then 'Réengagé'
            else state
        end as state_relabeled
    from with_context
),

-- Étape 2 : compteur global cumulé de Désengagé et Réengagé par enfant × cohorte
-- (le compteur global combiné à la phase garantit un DAG)
with_cumulative_counts as (
    select
        *,
        sum(case when state_relabeled = 'Désengagé' then 1 else 0 end) over (
            partition by child_id, cohort_id
            order by step_number
            rows between unbounded preceding and current row
        ) as disengage_count,
        sum(case when state_relabeled = 'Réengagé' then 1 else 0 end) over (
            partition by child_id, cohort_id
            order by step_number
            rows between unbounded preceding and current row
        ) as reengage_count
    from relabeled_events
),

-- Étape 3 : libellé final = "Désengagé #N (P_phase)" / "Réengagé #N (P_phase)"
-- N = compteur global (1ère, 2ème désengagement, etc.), P = phase de la cohorte
labeled_events as (
    select
        child_id,
        cohort_id,
        cohort_name,
        step_number,
        event_date,
        disengagement_type,
        case
            when state_relabeled = 'Désengagé'
                then 'Désengagé #' || disengage_count::string || ' (P' || phase::string || ')'
            when state_relabeled = 'Réengagé'
                then 'Réengagé #' || reengage_count::string || ' (P' || phase::string || ')'
            else state_relabeled
        end as state_display
    from with_cumulative_counts
),

with_previous as (
    select
        *,
        lag(state_display) over (
            partition by child_id, cohort_id
            order by step_number
        ) as previous_state
    from labeled_events
)

select
    child_id,
    cohort_id,
    cohort_name,
    step_number,
    event_date,
    disengagement_type,
    coalesce(previous_state, 'Début')   as sankey_source,
    state_display                        as sankey_target
from with_previous
-- Dédoublonner les self-loops (X → X)
where state_display is distinct from previous_state
order by child_id, cohort_id, step_number
