with p as (
    select *
    from {{ ref('stg_1001mots_app__parents') }}
)

select
    parent_id,
    gender,
    first_name,
    last_name,
    email,
    phone_number,
    phone_number_national,
    address,
    postal_code,
    left(postal_code, 2) as departement,
    city_name,
    date_created,
    date_updated,
    date_terms_accepted,
    is_ambassador,
    job,
    letterbox_name,
    degree,
    mid_term_rate,
    mid_term_reaction
from p
