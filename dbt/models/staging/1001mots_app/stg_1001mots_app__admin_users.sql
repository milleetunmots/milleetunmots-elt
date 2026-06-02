with source as (
    select *
    from {{ source('mots_app', 'admin_users') }}
)

select
    id::string as supporter_id,
    email::string as email,
    name::string as name,
    user_role::string as user_role,
    is_disabled::boolean as is_disabled,
    can_treat_task::boolean as can_treat_task,
    can_send_automatic_sms::boolean as can_send_automatic_sms,
    aircall_phone_number::string as aircall_phone_number,
    aircall_number_id::integer as aircall_number_id,
    sign_in_count::integer as sign_in_count,
    to_timestamp(
            nullif(remember_created_at::string, '')
    ) as date_remember_created,
    to_date(
            nullif(created_at::string, '')
    ) as date_created,
    to_date(
            nullif(updated_at::string, '')
    ) as date_updated
from source