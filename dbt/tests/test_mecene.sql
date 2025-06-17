{# in dbt Develop #}


{% set old_mecene_query %}
select
    source_channel,
    source_name,
    family_postal_code,
    family_city,
    year_of_registration,
    departement,
    child_id,
    is_a_father,
    month_of_registration,
    date_of_registration,
    child_age_in_month,
    accompagnement_annee_n_moins_2,
    accompagnement_annee_n_moins_1,
    accompagnement_annee_n_moins_1_decompose,
    accompagnement_annee_n,
    accompagnement_annee_n_decompose,
    accompagnement_annee_n_1,
    accompagnement_annee_n_1_ajuste,
    accompagnement_annee_n_1_ajuste_decompose
from source('mots_dl', 'benchmark_mecene')
{% endset %}


{% set new_mecene_query %}
select
    source_channel,
    source_name,
    family_postal_code,
    family_city,
    year_of_registration,
    departement,
    child_id,
    is_a_father,
    month_of_registration,
    date_of_registration,
    child_age_in_month,
    accompagnement_annee_n_moins_2,
    accompagnement_annee_n_moins_1,
    accompagnement_annee_n_moins_1_decompose,
    accompagnement_annee_n,
    accompagnement_annee_n_decompose,
    accompagnement_annee_n_1,
    accompagnement_annee_n_1_ajuste,
    accompagnement_annee_n_1_ajuste_decompose
from {{ ref('mecene') }}
{% endset %}


{{ audit_helper.compare_queries(
    a_query=old_mecene_query,
    b_query=new_mecene_query,
    primary_key="child_id"
) }}