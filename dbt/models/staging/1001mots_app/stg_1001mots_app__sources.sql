with source as (
    select *
    from {{ source('mots_app', 'sources') }}
)

select
    id::string as source_id,
    name::string as name,
    channel::string as channel,
    department::string as department,
    utm::string as utm,
    comment::string as comment,
    to_date(
            nullif(created_at::string, '')
    ) as date_created,
    to_date(
            nullif(updated_at::string, '')
    ) as date_updated,
    is_archived::boolean as is_archived
from source
