with super_table as (
    select * from {{ ref('super_table') }}
),

-- Dépivotage de super_table : 1 ligne par famille × session
super_table_long as (
    select
        family_id,
        child_id,
        cohort_name,
        supporter_name,
        group_status,
        0                       as call_session,
        call_0_status           as call_status,
        nb_of_tries_call0       as nb_of_tries,
        was_engaged_at_call0    as was_engaged
    from super_table
    where nb_of_tries_call0 is not null

    union all

    select
        family_id,
        child_id,
        cohort_name,
        supporter_name,
        group_status,
        1,
        call1_status,
        nb_of_tries_call1,
        was_engaged_at_call1
    from super_table
    where nb_of_tries_call1 is not null

    union all

    select
        family_id,
        child_id,
        cohort_name,
        supporter_name,
        group_status,
        2,
        call2_status,
        nb_of_tries_call2,
        was_engaged_at_call2
    from super_table
    where nb_of_tries_call2 is not null

    union all

    select
        family_id,
        child_id,
        cohort_name,
        supporter_name,
        group_status,
        3,
        call3_status,
        nb_of_tries_call3,
        was_engaged_at_call3
    from super_table
    where nb_of_tries_call3 is not null
),

-- Agrégation des RDVs par famille × session
bookings as (
    select
        family_id,
        call_session,
        count(*)                                                            as nb_bookings,
        sum(case when status = 'canceled' then 1 else 0 end)               as nb_canceled,
        max(case when status = 'scheduled' then 1 else 0 end)              as has_active_booking,
        max(case when status = 'canceled' then 1 else 0 end)               as has_canceled_booking
    from {{ ref('scheduled_calls') }}
    group by 1, 2
)

select
    st.family_id,
    st.child_id,
    st.cohort_name,
    st.supporter_name,
    st.group_status,
    st.call_session,
    st.call_status,
    st.nb_of_tries,
    st.was_engaged,

    -- Indicateurs de RDV
    coalesce(b.nb_bookings, 0)                                              as nb_bookings,
    coalesce(b.nb_canceled, 0)                                              as nb_canceled,
    coalesce(b.has_active_booking, 0)                                       as has_active_booking,
    coalesce(b.has_canceled_booking, 0)                                     as has_canceled_booking,
    case when coalesce(b.nb_bookings, 0) > 0 then 1 else 0 end             as has_booking,

    -- RDV reprogrammé = annulé ET nouveau RDV actif
    case
        when coalesce(b.has_canceled_booking, 0) = 1
         and coalesce(b.has_active_booking, 0) = 1
        then 1 else 0
    end                                                                     as is_rescheduled,

    -- Annulé sans reprogrammation
    case
        when coalesce(b.has_canceled_booking, 0) = 1
         and coalesce(b.has_active_booking, 0) = 0
        then 1 else 0
    end                                                                     as is_canceled_only

from super_table_long as st
left join bookings as b
    on b.family_id = st.family_id
    and b.call_session = st.call_session
