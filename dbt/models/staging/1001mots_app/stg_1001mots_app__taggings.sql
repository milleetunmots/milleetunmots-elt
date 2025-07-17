with source as (
    select *
    from {{ source('mots_app', 'taggings') }}
)

select
    id::string as tagging_id,
    tag_id::string as tag_id,
    taggable_id::string as taggable_id,
    taggable_type::string as taggable_type,
    context::string as context,
    to_date(
            nullif(created_at::string, '')
    ) as date_created
from source