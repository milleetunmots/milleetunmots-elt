with source as (
    select *
    from {{ source('seed', 'seed_mecene') }}
)

select
    concat(family_id, '_', child_id) as ind,
    family_id::integer as family_id,
    cohort_name::string as cohort_name,
    source_channel::string as source_channel,
    source_name::string as source_name,
    family_postal_code::string as family_postal_code,
    family_city::string as family_city,
    null as year_of_registration,
    departement::string as departement,
    child_id::string as child_id,
    is_a_father::integer as is_a_father,
    null as month_of_registration,
    null as date_of_registration,
    child_age_in_month::string as child_age_in_month,
    accompagnement_annee_n_moins_2::float as accompagnement_annee_n_moins_2,
    accompagnement_annee_n_moins_1::float as accompagnement_annee_n_moins_1,
    accompagnement_annee_n_moins_1_decompose::string as accompagnement_annee_n_moins_1_decompose,
    accompagnement_annee_n::float as accompagnement_annee_n,
    accompagnement_annee_n_decompose::string as accompagnement_annee_n_decompose,
    accompagnement_annee_n_1::float as accompagnement_annee_n_1,
    accompagnement_annee_n_1_ajuste::float as accompagnement_annee_n_1_ajuste,
    accompagnement_annee_n_1_ajuste_decompose::string as accompagnement_annee_n_1_ajuste_decompose
from source
