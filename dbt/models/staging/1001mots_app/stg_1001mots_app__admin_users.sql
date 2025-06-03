with source as (
    select *
    from {{ source('1001mots_app', 'admin_users') }}
)

select
    id::string as admin_user_id,
    email::string as email,
    name::string as name,
    to_date(
            nullif(created_at::string, '')
    ) as date_created,
    to_date(
            nullif(updated_at::string, '')
    ) as date_updated,
    to_date(
            nullif(discarded_at::string, '')
    ) as date_discarded,
    user_role::string as user_role,
    is_disabled::boolean as is_disabled,
    can_treat_tasks::boolean as can_treat_tasks
    -- Ajouter les autres colonnes
from source