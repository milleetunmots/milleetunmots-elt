{% set old_relation = ref('seed_st') -%}

{% set dbt_relation = ref('super_table') %}

{{ audit_helper.compare_all_columns(
    a_relation = old_relation,
    b_relation = dbt_relation,
    primary_key = "ind"
) }}