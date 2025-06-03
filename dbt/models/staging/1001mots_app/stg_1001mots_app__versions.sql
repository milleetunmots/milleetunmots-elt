with source as (
    select *
    from {{ source('1001mots_app', 'versions') }}
)

select
    id::string as version_id,
    item_type::string as item_type,
    item_id::string as item_id,
    event::string as event,
    name::string as name,
    to_date(
            nullif(created_at::string, '')
    ) as date_created,
    object::string as object,
    object_changes::string as object_changes
from source