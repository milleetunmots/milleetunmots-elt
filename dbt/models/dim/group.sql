with g as (
    select *
    from {{ ref('stg_1001mots_app__groups') }}
)

select
    group_id,
    group_name,
    date_created,
    date_started,
    date_ended,
    case 
        when date_ended is null then date_started + interval '12 months' 
        else date_ended 
    end as date_ended_clean,
    date_updated,
    date_discarded,
    date_call0_start,
    date_call0_end,
    date_call1_start,
    date_call1_end,
    date_call2_start,
    date_call2_end,
    date_call3_start,
    date_call3_end,
    is_programmed,
    is_excluded_from_analytics
from g

