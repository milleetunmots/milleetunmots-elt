with source as (
    select *
    from {{ source('mots_app', 'child_supports') }}
)

select
    id::string as family_id,
    supporter_id::string as supporter_id,
    -- Call 0 fields
    call0_attempt::string as call0_attempt,
    call0_duration::integer as call0_duration,
    trim(call0_status::string) as call0_status,
    call0_review::string as call0_review,
    call0_goals_sms::string as call0_goals_sms,
    nullif(call0_goals::string, '') as call0_goals,
    -- Call 1 fields
    call1_attempt::string as call1_attempt,
    call1_duration::integer as call1_duration,
    trim(call1_status::string) as call1_status,
    call1_review::string as call1_review,
    call1_goals_sms::string as call1_goals_sms,
    nullif(call1_goals::string, '') as call1_goals,
    call1_previous_goals_follow_up::string as call1_previous_goals_follow_up,
    -- Call 2 fields
    call2_attempt::string as call2_attempt,
    call2_duration::integer as call2_duration,
    trim(call2_status::string) as call2_status,
    call2_review::string as call2_review,
    call2_goals_sms::string as call2_goals_sms,
    nullif(call2_goals::string, '') as call2_goals,
    call2_previous_goals_follow_up::string as call2_previous_goals_follow_up,
    -- Call 3 fields
    call3_attempt::string as call3_attempt,
    call3_duration::integer as call3_duration,
    trim(call3_status::string) as call3_status,
    call3_review::string as call3_review,
    call3_goals_sms::string as call3_goals_sms,
    nullif(call3_goals::string, '') as call3_goals,
    call3_previous_goals_follow_up::string as call3_previous_goals_follow_up,
    -- Call 4 fields
    call4_previous_goals_follow_up::string as call4_previous_goals_follow_up,
    -- Timestamps
    to_date(nullif(created_at::string, '')) as date_created,
    to_date(nullif(updated_at::string, '')) as date_updated,
    is_bilingual::string as is_bilingual,
    module2_chosen_by_parents_id::integer as module2_chosen_by_parents_id,
    module3_chosen_by_parents_id::integer as module3_chosen_by_parents_id,
    module4_chosen_by_parents_id::integer as module4_chosen_by_parents_id,
    module5_chosen_by_parents_id::integer as module5_chosen_by_parents_id,
    module6_chosen_by_parents_id::integer as module6_chosen_by_parents_id
from source