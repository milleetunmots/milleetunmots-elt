with source as (
    select *
    from {{ source('mots_app', 'events') }}
)

select
    id::string as event_id,
    workshop_id::string as workshop_id,
    related_type::string as related_type,
    related_id::int as related_id,
    to_date(
            nullif(created_at::string, '')
    ) as date_created,
    to_date(
            nullif(occurred_at::string, '')
    ) as date_occurred,
    to_date(
            nullif(acceptation_date::string, '')
    ) as date_accepted,
    to_date(
            nullif(discarded_at::string, '')
    ) as date_discarded,
    to_timestamp(
            nullif(updated_at::string, '')
    ) as date_updated,
    body::string as body,
    parent_response::string as parent_response,
    parent_presence::string as parent_presence
from source