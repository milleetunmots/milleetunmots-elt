{% macro get_call_review(column_name) %}
    CASE 
        WHEN {{ column_name }} = '0_very_satisfied'      THEN 'très satisfaisant' 
        WHEN {{ column_name }} = '1_rather_satisfied'   THEN 'satisfaisant' 
        WHEN {{ column_name }} = '2_rather_dissatisfied' THEN 'peu satisfaisant'  
        WHEN {{ column_name }} = '3_very_dissatisfied'  THEN 'très insatisfaisant' 
        WHEN {{ column_name }} = ''                     THEN 'vide' 
        ELSE {{ column_name }} 
    END
{% endmacro %} 