with source as (
    select *
    from {{ source('1001mots_app', 'child_supports') }}
)

select
    id::string as family_id,
    supporter_id::string as supporter_id
from source