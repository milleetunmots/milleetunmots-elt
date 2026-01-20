with source as (
    select *
    from {{ source('mots_app', 'books') }}
)

select
    id::string as book_id,
    title::string as title,
    ean::string as ean,
    media_id::string as media_id,
    to_date(
        nullif(created_at::string, '')
    ) as date_created,
    to_date(
        nullif(updated_at::string, '')
    ) as date_updated
from source