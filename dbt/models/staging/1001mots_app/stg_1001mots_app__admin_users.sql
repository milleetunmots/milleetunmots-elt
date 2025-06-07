with source as (
    select *
    from {{ source('mots_app', 'admin_users') }}
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
    user_role::string as user_role,
    is_disabled::boolean as is_disabled,
    can_treat_task::boolean as can_treat_task
    -- Ajouter les autres colonnes
from source