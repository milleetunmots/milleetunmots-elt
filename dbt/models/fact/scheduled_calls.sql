with aircall as (
    select * from {{ ref('stg_1001mots_app__aircall_calls') }}
),

super_table as (
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
    --where nb_of_tries_call0 is not null

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
    --where nb_of_tries_call1 is not null

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
    --where nb_of_tries_call2 is not null

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
    --where nb_of_tries_call3 is not null
),

-- Agrégation des RDVs par famille × session
-- avec flag si un appel aircall a eu lieu pendant le créneau booké
bookings as (
    select
        sc.family_id,
        sc.call_session,
        count(*)                                                            as nb_bookings,
        sum(case when sc.status = 'canceled' then 1 else 0 end)            as nb_canceled,
        max(case when sc.status = 'scheduled' then 1 else 0 end)           as has_active_booking,
        max(case when sc.status = 'canceled' then 1 else 0 end)            as has_canceled_booking,
        min(sc.date_scheduled)                                              as first_booking_date,
        max(sc.date_scheduled)                                              as last_booking_date,
        max(sc.date_canceled)                                               as last_canceled_date,
        -- Date du RDV reprogrammé = date du booking actif quand il y a aussi eu une annulation
        max(case when sc.status = 'scheduled' then sc.date_scheduled end)   as rescheduled_date,
        listagg(distinct sc.event_type_name, ', ')
            within group (order by sc.event_type_name)                     as event_type_names,
        -- Un appel a-t-il eu lieu pendant le créneau ?
        max(case
            when ac.started_at >= sc.scheduled_at
             and ac.started_at <= dateadd(minute, sc.duration_minutes, sc.scheduled_at)
            then 1 else 0
        end)                                                                as had_call_during_slot,
        -- Un vrai appel (duration > 400s) a-t-il eu lieu pendant le créneau ?
        max(case
            when ac.started_at >= sc.scheduled_at
             and ac.started_at <= dateadd(minute, sc.duration_minutes, sc.scheduled_at)
             and ac.duration > 300
            then 1 else 0
        end)                                                                as had_real_call_during_slot
    from {{ ref('stg_1001mots_app__scheduled_calls') }} as sc
    left join aircall as ac
        on  ac.child_support_id = sc.family_id
        and ac.call_session = sc.call_session
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
    end                                                                     as is_canceled_only,

    -- Détails du RDV
    b.first_booking_date,
    b.last_booking_date,
    b.last_canceled_date,
    case when coalesce(b.has_canceled_booking, 0) = 1
          and coalesce(b.has_active_booking, 0) = 1
         then b.rescheduled_date
    end                                                                     as rescheduled_date,
    b.event_type_names,
    coalesce(b.had_call_during_slot, 0)                                     as had_call_during_slot,
    coalesce(b.had_real_call_during_slot, 0)                                as had_real_call_during_slot,

    -- No-show côté aircall : RDV pris mais aucun vrai appel pendant le créneau
    case
        when coalesce(b.has_active_booking, 0) = 1
         and coalesce(b.had_real_call_during_slot, 0) = 0
        then 1 else 0
    end                                                                     as is_no_show_aircall,

    -- No-show côté statut d'appel : RDV pris mais appel ni OK (toute variante) ni Incomplet (NULL inclus)
    case
        when coalesce(b.has_active_booking, 0) = 1
         and not (
             coalesce(st.call_status, '') ilike '%ok%'
             or coalesce(st.call_status, '') ilike 'incomplet%'
         )
        then 1 else 0
    end                                                                     as is_no_show_call_status

from super_table_long as st
left join bookings as b
    on b.family_id = st.family_id
    and b.call_session = st.call_session
