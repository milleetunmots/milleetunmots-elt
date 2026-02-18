with source as (
    select *
    from {{ source('mots_app', 'workshops') }}
)

select
    id::string as workshop_id,
    name::string as workshop_name,
    workshop_land::string as workshop_land,
    to_date(
            nullif(created_at::string, '')
    ) as date_created,
    to_date(
            nullif(updated_at::string, '')
    ) as date_updated,
    to_date(
            nullif(workshop_date::string, '')
    ) as date_workshop,
    to_date(
            nullif(discarded_at::string, '')
    ) as date_discarded,
	topic::string as topic,
	co_animator::string as co_animator,
	animator_id::int as animator_id,
    postal_code::string as postal_code,
    location::string as location,
    city_name::string as city_name,
    TO_VARCHAR(
        CONVERT_TIMEZONE('UTC', 'Europe/Paris', 
            TO_TIMESTAMP(date_workshop::DATE || ' ' || first_workshop_time_slot)
        ), 
    'HH24:MI:SS')::string AS first_workshop_time_slot,
    TO_VARCHAR(
        CONVERT_TIMEZONE('UTC', 'Europe/Paris', 
            TO_TIMESTAMP(date_workshop::DATE || ' ' || second_workshop_time_slot)
        ), 
    'HH24:MI:SS')::string AS second_workshop_time_slot
    --second_workshop_time_slot::string as second_workshop_time_slot
from source