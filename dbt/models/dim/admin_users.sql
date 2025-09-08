with source as (
    select *
    from {{ ref('stg_1001mots_app__admin_users') }}
)

select
    supporter_id,
    email,
    name,
    date_created,
    date_updated,
    user_role,
    is_disabled,
    can_treat_task
from source 