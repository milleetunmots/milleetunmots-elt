with source as (
    select *
    from {{ source('mots_app', 'children') }}
)

select
    id::string as child_id,
    parent1_id::string as parent1_id,
    parent2_id::string as parent2_id,
    child_support_id::string as family_id,
    group_id::string as group_id,
    to_date(
            nullif(group_end::string, '')
    ) as date_group_end,
    first_name::string as first_name,
    last_name::string as last_name,
    to_date(
            nullif(birthdate::string, '')
    ) as date_birth,
    to_date(
            nullif(created_at::string, '')
    ) as date_created,
    to_date(
            nullif(updated_at::string, '')
    ) as date_updated,
    to_date(
            --nullif(discarded_at::string, '')
            discarded_at
    ) as date_discarded,
    gender::string as gender,
    should_contact_parent1::boolean as should_contact_parent1,
    should_contact_parent2::boolean as should_contact_parent2,
    registration_source_details::string as registration_source_details,
    registration_source::string as registration_source,
    family_redirection_urls_count::integer as family_redirection_urls_count,
    family_redirection_url_visits_count::integer as family_redirection_url_visits_count,
    group_status::string as group_status
    --Ajouter les autres colonnes
from source
