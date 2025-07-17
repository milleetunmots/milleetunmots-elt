with source as (
    select *
    from {{ source('mots_app', 'tags') }}
)

select
    id::string as tag_id,
    name::string as tag_name,
    to_date(
            nullif(created_at::string, '')
    ) as date_created,
    to_date(
            nullif(updated_at::string, '')
    ) as date_updated,
    taggings_count::integer as taggings_count,
    color::string as color,
    is_visible_by_callers::boolean as is_visible_by_callers
from source