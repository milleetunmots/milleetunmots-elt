with source as (
    select *
    from {{ source('mots_app', 'children_support_modules') }}
)

select
    id::string as module_id,
    child_id::string as child_id,
    parent_id::string as parent_id,
    module_index::integer as module_index,
    book_id::string as book_id,
    book_condition::string as book_condition,
    to_date(
        nullif(created_at::string, '')
    ) as date_created,
    to_date(
        nullif(updated_at::string, '')
    ) as date_updated
from source