
{% set old_relation = ref('seed_fa') -%}

{% set dbt_relation = ref('funnel_activation') %}

{{ audit_helper.compare_all_columns(
    a_relation = old_relation,
    b_relation = dbt_relation,
    primary_key = "id"
) }}