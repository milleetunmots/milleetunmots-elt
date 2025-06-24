{# in dbt Develop #}


{% set old_mecene_query %}
select
    source_channel::string as source_channel,
    source_name::string as source_name,
    family_postal_code::string as family_postal_code,
    family_city::string as family_city,
    --to_date(year_of_registration) as year_of_registration,
    departement::string as departement,
    child_id::string as child_id,
    is_a_father::integer as is_a_father,
    --to_date(month_of_registration) as month_of_registration,
    --to_date(date_of_registration) as date_of_registration,
    child_age_in_month::string as child_age_in_month,
    accompagnement_annee_n_moins_2::integer as accompagnement_annee_n_moins_2,
    -- accompagnement_annee_n_moins_1::integer as accompagnement_annee_n_moins_1,
    -- accompagnement_annee_n_moins_1_decompose::string as accompagnement_annee_n_moins_1_decompose,
    -- accompagnement_annee_n::integer as accompagnement_annee_n,
    -- accompagnement_annee_n_decompose::string as accompagnement_annee_n_decompose,
    -- accompagnement_annee_n_1::integer as accompagnement_annee_n_1,
    -- accompagnement_annee_n_1_ajuste::integer as accompagnement_annee_n_1_ajuste,
    -- accompagnement_annee_n_1_ajuste_decompose::float as accompagnement_annee_n_1_ajuste_decompose
from {{ source('seed', 'benchmark_mecene') }}
{% endset %}


{% set new_mecene_query %}
select
    source_channel,
    source_name,
    family_postal_code,
    family_city,
    --to_date(year_of_registration) as year_of_registration,
    departement,
    child_id,
    is_a_father,
    --to_date(month_of_registration) as month_of_registration,
    --to_date(date_of_registration) as date_of_registration,
    child_age_in_month,
    accompagnement_annee_n_moins_2,
    -- accompagnement_annee_n_moins_1,
    -- accompagnement_annee_n_moins_1_decompose,
    -- accompagnement_annee_n,
    -- accompagnement_annee_n_decompose,
    -- accompagnement_annee_n_1,
    -- accompagnement_annee_n_1_ajuste,
    -- accompagnement_annee_n_1_ajuste_decompose
from {{ ref('mecene') }}
{% endset %}


{{ audit_helper.compare_queries(
    a_query=old_mecene_query,
    b_query=new_mecene_query,
    primary_key="child_id"
) }}