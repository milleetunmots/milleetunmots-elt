with c as (
    select *
    from {{ ref('stg_1001mots_app__children') }}
),

cs as (
    select *
    from {{ ref('stg_1001mots_app__child_supports') }}
),

g as (
    select *
    from {{ ref('groups') }}
),

au as (
    select *
    from {{ ref('stg_1001mots_app__admin_users') }}
),

child_family as (
    select 
        cs.family_id, 
        max(c.child_id) as child_id, 
        max(c.group_id) as group_id 
    from c
    left join cs
        on c.family_id = cs.family_id
    group by 1 
),

all_calls as (
    select 
        family_id,
        'call 0' as call_number,
        supporter_id,
        call0_duration as duration_call,
        call0_status as call_done,
        call0_review as call_review,
        case when call0_goals_sms is not null and call0_goals_sms != '' then 1 else null end as pm_posee,
        case when call0_goals is not null and call0_goals != '' then 1 else null end as pm_posee_call,
        case when call1_previous_goals_follow_up in ('1_succeed', '2_tried') then 1 else null end as pm_posee_callx_status,
        0 as duo_call0_1_ok,
        0 as duo_call1_2_ok,
        0 as duo_call2_3_ok,
        null as duo_call0_ok_1_ko,
        null as duo_call0_ok_1_ok
    from cs

    union all 

    select 
        family_id,
        'call 1' as call_number,
        supporter_id,
        call1_duration as duration_call,
        call1_status as call_done,
        call1_review as call_review,
        case when call1_goals_sms is not null and call1_goals_sms != '' then 1 else null end as pm_posee,
        case when call1_goals is not null and call1_goals != '' then 1 else null end as pm_posee_call,
        case when call2_previous_goals_follow_up in ('1_succeed', '2_tried') then 1 else null end as pm_posee_callx_status,
        case when lower(call0_status) = 'ok' and lower(call1_status) = 'ok' then 1 else 0 end as duo_call0_1_ok,
        0 as duo_call1_2_ok,
        0 as duo_call2_3_ok,
        case when lower(call0_status) = 'ok' and lower(call1_status) = 'ko' then 1 when lower(call0_status) = 'ok' then 0 else null end as duo_call0_ok_1_ko,
        case when lower(call0_status) = 'ok' and lower(call1_status) = 'ok' then 1 when lower(call0_status) = 'ok' then 0 else null end as duo_call0_ok_1_ok
    from cs

    union all 

    select 
        family_id,
        'call 2' as call_number,
        supporter_id,
        call2_duration as duration_call,
        call2_status as call_done,
        call2_review as call_review,
        case when call2_goals_sms is not null and call2_goals_sms != '' then 1 else null end as pm_posee,
        case when call2_goals is not null and call2_goals != '' then 1 else null end as pm_posee_call,
        case when call3_previous_goals_follow_up in ('1_succeed', '2_tried') then 1 else null end as pm_posee_callx_status,
        0 as duo_call0_1_ok,
        case when lower(call1_status) = 'ok' and lower(call2_status) = 'ok' then 1 else 0 end as duo_call1_2_ok,
        0 as duo_call2_3_ok,
        null as duo_call0_ok_1_ko, 
        null as duo_call0_ok_1_ok
    from cs

    union all 

    select 
        family_id,
        'call 3' as call_number,
        supporter_id,
        call3_duration as duration_call,
        call3_status as call_done,
        call3_review as call_review,
        case when call3_goals_sms is not null and call3_goals_sms != '' then 1 else null end as pm_posee,
        case when call3_goals is not null and call3_goals != '' then 1 else null end as pm_posee_call,
        case when call4_previous_goals_follow_up in ('1_succeed', '2_tried') then 1 else null end as pm_posee_callx_status,
        0 as duo_call0_1_ok,
        0 as duo_call1_2_ok,
        case when lower(call2_status) = 'ok' and lower(call3_status) = 'ok' then 1 else 0 end as duo_call2_3_ok,
        null as duo_call0_ok_1_ko,
        null as duo_call0_ok_1_ok
    from cs
)

select 
    cf.family_id,
    au.name as supporter_name, 
    au.email, 
    g.group_name as cohort_name,
    g.date_started as started_at,
    g.is_excluded_from_analytics,
    ac.call_number,
    ac.duration_call,
    ac.call_done,
    ac.pm_posee,
    ac.pm_posee_call,
    case 
        when ac.call_review = '0_very_satisfied' then 'très satisfaisant' 
        when ac.call_review = '1_rather_satisfied' then 'satisfaisant' 
        when ac.call_review = '2_rather_dissatisfied' then 'peu satisfaisant'  
        when ac.call_review = '3_very_dissatisfied' then 'très insatisfaisant' 
        else ac.call_review 
    end as call_review,
    ac.pm_posee_callx_status,
    ac.duo_call0_1_ok,
    ac.duo_call1_2_ok,
    ac.duo_call2_3_ok,
    ac.duo_call0_ok_1_ko,
    ac.duo_call0_ok_1_ok,
    au.is_disabled
from all_calls as ac 
left join child_family as cf
    on ac.family_id = cf.family_id
left join g
    on cf.group_id = g.group_id 
left join au
    on au.supporter_id = ac.supporter_id
where au.name is not null 
and not g.is_excluded_from_analytics