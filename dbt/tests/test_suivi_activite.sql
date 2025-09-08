{% set old_relation = ref('tsua') -%}

{% set dbt_relation = ref('suivi_activite2') %}

{{ audit_helper.compare_all_columns(
    a_relation = old_relation,
    b_relation = dbt_relation,
    primary_key = "unique_id"
) }}