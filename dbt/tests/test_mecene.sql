
{% set old_relation = ref('seed_tmp') -%}

{% set dbt_relation = ref('mecene') %}

{{ audit_helper.compare_all_columns(
    a_relation = old_relation,
    b_relation = dbt_relation,
    primary_key = "ind"
) }}