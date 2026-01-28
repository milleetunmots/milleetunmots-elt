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
    type::string as type,
    parent_response::string as parent_response,
    parent_presence::string as parent_presence,
    workshop_time_slot::string as workshop_time_slot,
    originated_by_app::boolean as originated_by_app,
    spot_hit_status::string as spot_hit_status,
    spot_hit_message_id::string as spot_hit_message_id
from source