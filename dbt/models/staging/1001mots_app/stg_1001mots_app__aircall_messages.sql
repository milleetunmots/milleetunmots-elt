with source as (
    select *
    from {{ source('mots_app', 'aircall_messages') }}
)

select
    id::string as call_id,
    aircall_id::string as aircall_id,
    child_support_id::int as child_support_id,
    direction::string as direction,
    parent_id::integer as parent_id,
    caller_id::string as caller_id,
    --sent_at::timestamp as sent_at,
    body::string as body,
    status::string as status,
    created_at::timestamp as created_at,
    to_date(
            nullif(created_at::string, '')
    ) as date_created,
    updated_at::timestamp as updated_at
from source

