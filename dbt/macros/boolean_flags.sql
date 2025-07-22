{% macro get_boolean_flag(column_name) %}
    CASE WHEN {{ column_name }} IS NULL OR {{ column_name }} = '' THEN 0 ELSE 1 END
{% endmacro %}

{% macro get_number_of_calls(call0_status, call1_status, call2_status, call3_status) %}
    {{ call0_status }} + {{ call1_status }} + {{ call2_status }} + {{ call3_status }}
{% endmacro %}

{% macro get_is_call_0_1_status_OK(call0_status, call1_status) %}
    ({{ call0_status }} = 'OK' OR {{ call1_status }} = 'OK')
{% endmacro %} 