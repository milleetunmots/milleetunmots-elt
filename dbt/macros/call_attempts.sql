{% macro get_call_attempt(column_name) %}
    CASE 
        WHEN {{ column_name }} = 'first_call_attempt'         THEN '1ère tentative'
        WHEN {{ column_name }} = 'second_call_attempt'        THEN '2ème tentative'
        WHEN {{ column_name }} = 'third_or_more_calls_attempt' THEN '3ème tentative'
        ELSE {{ column_name }} 
    END
{% endmacro %} 