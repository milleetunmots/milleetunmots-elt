with source as (
    select *
    from {{ source('mots_app', 'support_modules') }}
)

select
    id::string as module_id,
    name::string as name,
    to_date(
            nullif(created_at::string, '')
    ) as date_created,
    to_date(
            nullif(updated_at::string, '')
    ) as date_updated,
    to_date(
            nullif(discarded_at::string, '')
    ) as date_discarded,
    to_date(
            nullif(start_at::string, '')
    ) as date_start,
    for_bilingual::boolean as for_bilingual,
    theme::string as theme,
    -- A reprendre
    age_ranges::string as age_range,
    level::string as level,
    book_id::string as book_id
from source