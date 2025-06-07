with source as (
    select *
    from {{ source('mots_app', 'parents') }}
)

select
    id::string as parent_id,
    gender::string as gender,
    first_name::string as first_name,
    last_name::string as last_name,
    email::string as email,
    phone_number::string as phone_number,
    phone_number_national::string as phone_number_national,
    address::string as address,
    postal_code::string as postal_code,
    city_name::string as city_name,
    to_date(
            nullif(created_at::string, '')
    ) as date_created,
    to_date(
            nullif(updated_at::string, '')
    ) as date_updated,
    to_date(
            nullif(terms_accepted_at::string, '')
    ) as date_terms_accepted,
    is_ambassador::boolean as is_ambassador,
    job::string as job,
    letterbox_name::string as letterbox_name,
    degree::string as degree,
    mid_term_rate::integer as mid_term_rate,
    mid_term_reaction::string as mid_term_reaction
    --Ajouter les autres colonnes
from source