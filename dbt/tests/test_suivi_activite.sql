{# in dbt Develop #}


{% set old_suivi_qualite_query %}
select
    concat(family_id::string, '-', replace(call_number::string, ' ', '')) as id,
    family_id,
    supporter_name, 
    email, 
    replace(cohort_name::string, ' ', '') as cohort_name,
    --started_at,
    -- is_excluded_from_analytics,
    call_number,
    duration_call,
    call_done,
    pm_posee,
    pm_posee_call,
    call_review,
    pm_posee_callx_status,
    duo_call0_1_ok,
    duo_call1_2_ok,
    duo_call2_3_ok,
    duo_call0_ok_1_ko,
    duo_call0_ok_1_ok,
    is_disabled
from {{ source('seed', 'suivi_activite') }}
{% endset %}


{% set new_suivi_qualite_query %}
select
    concat(family_id::string, '-', replace(call_number::string, ' ', '')) as id,
    family_id,
    supporter_name, 
    email, 
    replace(cohort_name::string, ' ', '') as cohort_name,
    --started_at,
    -- is_excluded_from_analytics,
    call_number,
    duration_call,
    call_done,
    pm_posee,
    pm_posee_call,
    call_review,
    pm_posee_callx_status,
    duo_call0_1_ok,
    duo_call1_2_ok,
    duo_call2_3_ok,
    duo_call0_ok_1_ko,
    duo_call0_ok_1_ok,
    is_disabled
from {{ ref('suivi_qualite') }}
{% endset %}


{{ audit_helper.compare_column_values(
    a_query = old_suivi_qualite_query,
    b_query = new_suivi_qualite_query,
    primary_key = "id",
    column_to_compare = "duo_call0_ok_1_ok"
) }}
