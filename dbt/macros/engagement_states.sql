{% macro get_engagement_state_t1(t3_tag_id, t4_tag_id, t5_tag_id) %}
    CASE 
        WHEN {{ t3_tag_id }} IS NOT NULL THEN 'Désengagé t1'
        WHEN {{ t4_tag_id }} IS NOT NULL THEN 'Estime désengagé t1 conservé'
        WHEN {{ t5_tag_id }} IS NOT NULL THEN 'Estime désengagé t1'
        ELSE 'Conservé t1'
    END
{% endmacro %}

{% macro get_engagement_state_t2(t1_tag_id, t2_tag_id, t6_tag_id) %}
    CASE 
        WHEN {{ t1_tag_id }} IS NOT NULL THEN 'Désengagé t2'
        WHEN {{ t2_tag_id }} IS NOT NULL THEN 'Estime désengagé t2'
        WHEN {{ t6_tag_id }} IS NOT NULL THEN 'Estime désengagé t2 conservé'
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