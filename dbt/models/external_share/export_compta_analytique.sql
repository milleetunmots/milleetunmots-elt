with m as (
    select *
    from {{ ref('mecene') }}
)

select
    *
from m
where date_cohort_started between current_date - interval '180 days' and current_date
and child_status in ('active')