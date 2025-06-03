with source as (
    select *
    from {{ source('1001mots_app', 'children_sources') }}
)

select
    id::string as children_sources_id,
    source_id::string as source_id,
    child_id::string as child_id,
    to_date(
            nullif(created_at::string, '')
    ) as date_created,
    to_date(
            nullif(updated_at::string, '')
    ) as date_updated,
    details::string as details,
    registration_department::string as registration_department
from source