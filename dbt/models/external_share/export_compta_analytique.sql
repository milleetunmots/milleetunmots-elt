with m as (
    select *
    from {{ ref('mecene') }}
)

select
    *
from m
where date_cohort_started >= current_date - interval '180 days'
and child_status in ('active')