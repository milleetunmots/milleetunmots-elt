with am as (
    select *
    from {{ ref('stg_1001mots_app__aircall_messages') }}
),

ac as (
    select *
    from {{ ref('stg_1001mots_app__aircall_calls') }}
),

g as (
    select *
    from {{ ref('groups') }}
),

st as (
    select *
    from {{ ref('super_table') }}
),

base as (
    select
        family_id,
        cohort_name,
        supporter_name,
        0 as call_session,
        call_0_status as call_status,
        is_call0_goals as is_call_goals
    from st
    --where replace(call_0_status, ' ', '') != ''
    where call_0_status is not null and call_0_status != ''
    union all
    select
        family_id,
        cohort_name,
        supporter_name,
        1 as call_session,
        call1_status as call_status,
        is_call1_goals as is_call_goals
    from st
    --where replace(call1_status, ' ', '') != ''
    where call1_status is not null and call1_status != ''
    union all
    select
        family_id,
        cohort_name,
        supporter_name,
        2 as call_session,
        call2_status as call_status,
        is_call2_goals as is_call_goals
    from st
    --where replace(call2_status, ' ', '') != ''
    where call2_status is not null and call2_status != ''
    union all
    select
        family_id,
        cohort_name,
        supporter_name,
        3 as call_session,
        call3_status as call_status,
        is_call3_goals as is_call_goals
    from st
    --where replace(call3_status, ' ', '') != ''
    where call3_status is not null and call3_status != ''
),

g_base as (
    select
        group_id,
        group_name,
        0 as call_session,
        date_call0_start as started_at,
        date_call0_end as ended_at,
        is_excluded_from_analytics
    from g
    union all
    select
        group_id,
        group_name,
        1 as call_session,
        date_call1_start as started_at,
        date_call1_end as ended_at,
        is_excluded_from_analytics
    from g
    union all
    select
        group_id,
        group_name,
        2 as call_session,
        date_call2_start as started_at,
        date_call2_end as ended_at,
        is_excluded_from_analytics
    from g
    union all
    select
        group_id,
        group_name,
        3 as call_session,
        date_call3_start as started_at,
        date_call3_end as ended_at,
        is_excluded_from_analytics
    from g
),

final as (
select
    --ac.call_id,
    base.cohort_name,
    base.supporter_name,
    base.call_session,
    base.call_status,
    base.family_id,
    base.is_call_goals,
    max(ac.duration) as max_duration,
    max(ac.duration) > 250 as is_real_call,
    sum(case when ac.direction = 'outbound' then 1 else 0 end) as nb_of_calls_sent,
    sum(case when ac.direction = 'inbound' then 1 else 0 end) as nb_of_calls_received,
    sum(case when ac.duration > 250 and ac.direction = 'outbound' then 1 else 0 end) as nb_of_real_calls_sent,
    sum(case when ac.duration > 250 and ac.direction = 'inbound' then 1 else 0 end) as nb_of_real_calls_received,
    count(distinct am.call_id) as nb_of_messages_sent
from base
left join g_base
    on base.cohort_name = g_base.group_name
    and base.call_session = g_base.call_session
left join ac
    on ac.date_started between g_base.started_at and g_base.ended_at
    and base.family_id = ac.child_support_id
left join am
    on am.date_created between g_base.started_at - 2 and g_base.ended_at
    and base.family_id = am.child_support_id
where not is_excluded_from_analytics
and g_base.ended_at <= current_date
group by 1,2,3,4,5,6
)

select
    final.*,
    ac.call_id
from final
left join g_base
    on final.cohort_name = g_base.group_name
    and final.call_session = g_base.call_session
left join ac
    on final.family_id = ac.child_support_id
    and final.max_duration = ac.duration
    and ac.date_started between g_base.started_at and g_base.ended_at
