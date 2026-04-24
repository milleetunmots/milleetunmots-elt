{% macro get_engagement_state_t1(t3_tag_id, t4_tag_id, t5_tag_id) %}
    CASE 
        WHEN {{ t3_tag_id }} IS NOT NULL THEN 'Désengagé t1'
        WHEN {{ t4_tag_id }} IS NOT NULL THEN 'Estimé désengagé t1 conservé'
        --WHEN {{ t5_tag_id }} IS NOT NULL THEN 'Estime désengagé t1'
        ELSE 'Conservé t1'
    END
{% endmacro %}

{% macro get_engagement_state_t2(t1_tag_id, t2_tag_id, t6_tag_id) %}
    CASE 
        WHEN {{ t1_tag_id }} IS NOT NULL THEN 'Désengagé t2'
        --WHEN {{ t2_tag_id }} IS NOT NULL THEN 'Estime désengagé t2'
        WHEN {{ t6_tag_id }} IS NOT NULL THEN 'Estimé désengagé t2 conservé'
        ELSE 'Conservé t2'
    END
{% endmacro %}

{% macro get_is_bilingual(column_name) %}
    CASE 
        WHEN {{ column_name }} = '0_yes' THEN 'Oui'
        WHEN {{ column_name }} = '1_no'  THEN 'Non'
        ELSE NULL
    END
{% endmacro %}

{% macro get_call_number_when_disengaged(t7_date_created, date_call0_end, date_call1_end, date_call2_end, date_call3_end) %}
    CASE 
        when {{ t7_date_created }} is null then null
        when {{ t7_date_created }} > {{ date_call3_end }}  THEN 4
        WHEN {{ t7_date_created }} > {{ date_call2_end }} THEN 3
        WHEN {{ t7_date_created }} > {{ date_call1_end }} THEN 2
        WHEN {{ t7_date_created }} > {{ date_call0_end }} THEN 1
        ELSE 0
    END
{% endmacro %}

{% macro was_engaged_at_call(date_call_end, group_status) %}
    CASE 
        WHEN {{ date_call_end }} IS NULL THEN NULL
        WHEN {{ group_status }} = 'active' THEN 1
        WHEN {{ group_status }} = 'stopped' THEN 0
        ELSE NULL
    END
{% endmacro %}