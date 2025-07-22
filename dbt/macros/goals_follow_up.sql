{% macro get_goals_follow_up(column_name) %}
    CASE 
        WHEN {{ column_name }} IS NULL OR {{ column_name }} = '' THEN NULL
        WHEN {{ column_name }} = '1_succeed' THEN 'PM réussie'
        WHEN {{ column_name }} = '2_tried' THEN 'PM essayée'
        WHEN {{ column_name }} = '3_no_tried' THEN 'PM non essayée'
        WHEN {{ column_name }} = '4_no_goal' THEN 'Pas de PM' 
        ELSE {{ column_name }} 
    END
{% endmacro %} 