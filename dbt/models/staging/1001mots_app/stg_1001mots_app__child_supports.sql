with source as (
    select *
    from {{ source('mots_app', 'child_supports') }}
),

source2 as (
    select 
        *,
        parse_json(object) as object_json,
        parse_json(object_changes) as object_json_changes
    from {{ source('mots_app', 'versions') }}
),

filterr as (
    select
        id,
        item_id,
        row_number() over (partition by item_id order by created_at desc) as rn
    from source2
    where item_type = 'ChildSupport'
    qualify rn = 1
)

-- Il faut intégrer les données du object_json_changes dans le object_json
select
    coalesce(source.id::string, object_json:id::string) as family_id,
    --source.supporter_id::string as supporter_id,
    coalesce(object_json:supporter_id::string, source.supporter_id::string) as supporter_id,
    -- Call 0 fields
    coalesce(object_json:call0_attempt::string, source.call0_attempt::string) as call0_attempt,
    coalesce(object_json:call0_duration::integer, source.call0_duration::integer) as call0_duration,
    coalesce(nullif(trim(rtrim(object_json:call0_status::string)), ''), source.call0_status::string) as call0_status,
    coalesce(object_json:call0_review::string, source.call0_review::string) as call0_review,
    coalesce(object_json:call0_goals_sms::string, source.call0_goals_sms::string) as call0_goals_sms,
    coalesce(nullif(object_json:call0_goals::string, ''), source.call0_goals::string) as call0_goals,
    -- Call 1 fields
    coalesce(object_json:call1_attempt::string, source.call1_attempt::string) as call1_attempt,
    coalesce(object_json:call1_duration::integer, source.call1_duration::integer) as call1_duration,
    coalesce(nullif(trim(rtrim(object_json:call1_status::string)), ''), source.call1_status::string) as call1_status,
    coalesce(object_json:call1_review::string, source.call1_review::string) as call1_review,
    coalesce(object_json:call1_goals_sms::string, source.call1_goals_sms::string) as call1_goals_sms,
    coalesce(nullif(object_json:call1_goals::string, ''), source.call1_goals::string) as call1_goals,
    coalesce(object_json:call1_previous_goals_follow_up::string, source.call1_previous_goals_follow_up::string) as call1_previous_goals_follow_up,
    -- Call 2 fields
    coalesce(object_json:call2_attempt::string, source.call2_attempt::string) as call2_attempt,
    coalesce(object_json:call2_duration::integer, source.call2_duration::integer) as call2_duration,
    coalesce(nullif(trim(rtrim(object_json:call2_status::string)), ''), source.call2_status::string) as call2_status,
    coalesce(object_json:call2_review::string, source.call2_review::string) as call2_review,
    coalesce(object_json:call2_goals_sms::string, source.call2_goals_sms::string) as call2_goals_sms,
    coalesce(nullif(object_json:call2_goals::string, ''), source.call2_goals::string) as call2_goals,
    coalesce(object_json:call2_previous_goals_follow_up::string, source.call2_previous_goals_follow_up::string) as call2_previous_goals_follow_up,
    -- Call 3 fields
    coalesce(object_json:call3_attempt::string, source.call3_attempt::string) as call3_attempt,
    coalesce(object_json:call3_duration::integer, source.call3_duration::integer) as call3_duration,
    coalesce(nullif(trim(rtrim(object_json:call3_status::string)), ''), source.call3_status::string) as call3_status,
    coalesce(object_json:call3_review::string, source.call3_review::string) as call3_review,
    coalesce(object_json:call3_goals_sms::string, source.call3_goals_sms::string) as call3_goals_sms,
    coalesce(nullif(object_json:call3_goals::string, ''), source.call3_goals::string) as call3_goals,
    coalesce(object_json:call3_previous_goals_follow_up::string, source.call3_previous_goals_follow_up::string) as call3_previous_goals_follow_up,
    -- Call 4 fields
    coalesce(object_json:call4_previous_goals_follow_up::string, source.call4_previous_goals_follow_up::string) as call4_previous_goals_follow_up,
    -- Timestamps
    coalesce(to_date(nullif(object_json:created_at::string, '')), to_date(nullif(source.created_at::string, ''))) as date_created,
    coalesce(to_date(nullif(object_json:updated_at::string, '')), to_date(nullif(source.updated_at::string, ''))) as date_updated,
    coalesce(object_json:is_bilingual::string, source.is_bilingual::string) as is_bilingual,
    coalesce(object_json:module2_chosen_by_parents_id::integer, source.module2_chosen_by_parents_id::integer) as module2_chosen_by_parents_id,
    coalesce(object_json:module3_chosen_by_parents_id::integer, source.module3_chosen_by_parents_id::integer) as module3_chosen_by_parents_id,
    coalesce(object_json:module4_chosen_by_parents_id::integer, source.module4_chosen_by_parents_id::integer) as module4_chosen_by_parents_id,
    coalesce(object_json:module5_chosen_by_parents_id::integer, source.module5_chosen_by_parents_id::integer) as module5_chosen_by_parents_id,
    coalesce(object_json:module6_chosen_by_parents_id::integer, source.module6_chosen_by_parents_id::integer) as module6_chosen_by_parents_id,
    nullif(replace(notes::string, ' ', ''), '') as notes,
    nullif(stop_support_reason::string, '') as stop_support_reason,
     to_date(
        nullif(stop_support_date::string, '')
    ) as date_stop_support
from source
left join source2
    on source.id = source2.item_id
left join filterr
    on source2.id = filterr.id
where (source2.item_id is null or filterr.rn = 1)