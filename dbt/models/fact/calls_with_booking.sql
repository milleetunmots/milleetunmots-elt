-- Granularité : 1 ligne par appel aircall
-- ⚠️  LIMITATION : ce modèle ne couvre que les appels passés via Aircall.
-- Les appels effectués en dehors d'Aircall (téléphone fixe, mobile direct, etc.)
-- ne sont pas visibles ici et ne peuvent pas être associés à un RDV Calendly.
-- Pour une vue complète au niveau session (incluant tous les appels), utiliser scheduled_calls.

with aircall as (
    select * from {{ ref('stg_1001mots_app__aircall_calls') }}
),

bookings as (
    select * from {{ ref('stg_1001mots_app__scheduled_calls') }}
),

super_table as (
    select * from {{ ref('super_table') }}
),

-- Dépivotage super_table pour contexte session
super_table_long as (
    select family_id, cohort_name, supporter_name, group_status,
           0 as call_session, call_0_status as call_status
    from super_table

    union all

    select family_id, cohort_name, supporter_name, group_status,
           1, call1_status
    from super_table

    union all

    select family_id, cohort_name, supporter_name, group_status,
           2, call2_status
    from super_table

    union all

    select family_id, cohort_name, supporter_name, group_status,
           3, call3_status
    from super_table
)

select
    -- Identifiants
    ac.call_id,
    ac.child_support_id                                     as family_id,
    ac.call_session,

    -- Contexte famille
    st.cohort_name,
    st.supporter_name,
    st.group_status,
    st.call_status,

    -- Détails de l'appel aircall
    ac.started_at,
    ac.date_started,
    ac.duration,
    ac.direction,
    ac.answered,
    ac.duration > 300                                       as is_real_call,

    -- RDV Calendly associé (si l'appel a eu lieu pendant le créneau)
    b.scheduled_call_id,
    b.scheduled_at,
    b.date_scheduled,
    b.duration_minutes                                      as booking_duration_minutes,
    b.status                                                as booking_status,
    b.event_type_name,
    b.invitee_name,
    b.invitee_email,
    b.date_canceled                                         as booking_date_canceled,
    b.cancellation_reason,
    (b.scheduled_call_id is not null)                       as had_booking

from aircall as ac
left join super_table_long as st
    on  st.family_id = ac.child_support_id
    and st.call_session = ac.call_session
left join bookings as b
    on  b.family_id = ac.child_support_id
    and b.call_session = ac.call_session
    and ac.started_at >= b.scheduled_at
    and ac.started_at <= dateadd(minute, b.duration_minutes, b.scheduled_at)
