{% macro get_age_range_at_start_of_cohort(birthdate, started_at) %}
    CASE 
        WHEN (DATE_PART('year', {{ started_at }}) - DATE_PART('year', {{ birthdate }})) * 12
            + (DATE_PART('month', {{ started_at }}) - DATE_PART('month', {{ birthdate }})) < 12
            THEN '0-11'
        WHEN (DATE_PART('year', {{ started_at }}) - DATE_PART('year', {{ birthdate }})) * 12
            + (DATE_PART('month', {{ started_at }}) - DATE_PART('month', {{ birthdate }})) < 24
            THEN '12-23'
        WHEN (DATE_PART('year', {{ started_at }}) - DATE_PART('year', {{ birthdate }})) * 12
            + (DATE_PART('month', {{ started_at }}) - DATE_PART('month', {{ birthdate }})) < 37
            THEN '24-36'
        ELSE NULL 
    END
{% endmacro %}

{% macro get_age_at_registration(created_at, birthdate) %}
    (DATE_PART('year', {{ created_at }}) - DATE_PART('year', {{ birthdate }})) * 12
        + (DATE_PART('month', {{ created_at }}) - DATE_PART('month', {{ birthdate }}))
{% endmacro %}

{% macro get_registration_delay(started_at, created_at) %}
    (DATE_PART('year', {{ started_at }}) - DATE_PART('year', {{ created_at }})) * 12
        + (DATE_PART('month', {{ started_at }}) - DATE_PART('month', {{ created_at }}))
{% endmacro %} 