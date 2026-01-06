with source as (
    select *
    from {{ source('mots_app', 'aircall_calls') }}
)

select
    id::string as call_id,
    child_support_id::int as child_support_id,
    parent_id::integer as parent_id,
    caller_id::string as caller_id,
    direction::string as direction,
    answered::boolean as answered,
    started_at::timestamp as started_at,
    to_date(
            nullif(started_at::string, '')
    ) as date_started,
    call_session::integer as call_session,
    duration::integer as duration,
    asset_url::string as asset_url,
    tags::variant as tags
from source

