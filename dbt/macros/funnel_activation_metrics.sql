-- Métriques de base pour call0
{% macro is_call0_ok(cs) %}
    CASE 
        WHEN {{ cs }}.call0_status = 'OK' 
        THEN 1 ELSE NULL 
    END
{% endmacro %}

{% macro is_call0_ko(cs) %}
    CASE 
        WHEN {{ cs }}.call0_status != 'OK' 
        THEN 1 ELSE NULL 
    END
{% endmacro %}

-- Métriques pour PM0 (goals SMS)
{% macro is_call0_ok_pm0_setup(cs) %}
    CASE 
        WHEN {{ cs }}.call0_status = 'OK' 
        AND {{ cs }}.call0_goals_sms IS NOT NULL AND {{ cs }}.call0_goals_sms != '' 
        THEN 1 ELSE NULL 
    END
{% endmacro %}

{% macro is_call0_ok_pm0_not_setup(cs) %}
    CASE 
        WHEN {{ cs }}.call0_status = 'OK' 
        AND ({{ cs }}.call0_goals_sms IS NULL OR {{ cs }}.call0_goals_sms = '') 
        THEN 1 ELSE NULL 
    END
{% endmacro %}

-- Métriques pour call1 avec PM0 setup
{% macro is_call0_ok_pm0_setup_call1_ok(cs) %}
    CASE 
        WHEN {{ cs }}.call0_status = 'OK' 
        AND {{ cs }}.call0_goals_sms IS NOT NULL AND {{ cs }}.call0_goals_sms != '' 
        AND {{ cs }}.call1_status = 'OK' 
        THEN 1 ELSE NULL 
    END
{% endmacro %}

{% macro is_call0_ok_pm0_setup_call1_ko(cs) %}
    CASE 
        WHEN {{ cs }}.call0_status = 'OK' 
        AND {{ cs }}.call0_goals IS NOT NULL AND {{ cs }}.call0_goals_sms != '' 
        AND {{ cs }}.call1_status != 'OK' 
        THEN 1 ELSE NULL 
    END
{% endmacro %}

-- Métriques pour PM0 follow-up avec call1 KO
{% macro is_call0_ok_pm0_setup_call1_ko_pm0_ok(cs) %}
    CASE 
        WHEN {{ cs }}.call0_status = 'OK' 
        AND {{ cs }}.call0_goals_sms IS NOT NULL AND {{ cs }}.call0_goals_sms != '' 
        AND {{ cs }}.call1_status != 'OK' 
        AND {{ cs }}.call1_previous_goals_follow_up IN ('1_succeed', '2_tried') 
        THEN 1 ELSE NULL 
    END
{% endmacro %}

{% macro is_call0_ok_pm0_setup_call1_ko_pm0_ko(cs) %}
    CASE 
        WHEN {{ cs }}.call0_status = 'OK' 
        AND {{ cs }}.call0_goals_sms IS NOT NULL AND {{ cs }}.call0_goals_sms != '' 
        AND {{ cs }}.call1_status != 'OK' 
        AND {{ cs }}.call1_previous_goals_follow_up NOT IN ('1_succeed', '2_tried') 
        THEN 1 ELSE NULL 
    END
{% endmacro %}

-- Métriques pour PM0 follow-up avec call1 OK
{% macro is_call0_ok_pm0_setup_call1_ok_pm0_ok(cs) %}
    CASE 
        WHEN {{ cs }}.call0_status = 'OK'
        AND {{ cs }}.call0_goals_sms IS NOT NULL AND {{ cs }}.call0_goals_sms != '' 
        AND {{ cs }}.call1_status = 'OK' 
        AND {{ cs }}.call1_previous_goals_follow_up IN ('1_succeed', '2_tried') 
        THEN 1 ELSE NULL 
    END
{% endmacro %}

{% macro is_call0_ok_pm0_setup_call1_ok_pm0_ko(cs) %}
    CASE
        WHEN {{ cs }}.call0_status = 'OK' 
        AND {{ cs }}.call0_goals_sms IS NOT NULL AND {{ cs }}.call0_goals_sms != '' 
        AND {{ cs }}.call1_status = 'OK' 
        AND {{ cs }}.call1_previous_goals_follow_up NOT IN ('1_succeed', '2_tried') 
        THEN 1 ELSE NULL 
    END
{% endmacro %}

-- Métriques pour PM1 setup avec PM0 OK
{% macro is_call0_ok_pm0_setup_call1_ok_pm0_ok_pm1_setup(cs) %}
    CASE 
        WHEN {{ cs }}.call0_status = 'OK' 
        AND {{ cs }}.call0_goals_sms IS NOT NULL AND {{ cs }}.call0_goals_sms != '' 
        AND {{ cs }}.call1_status = 'OK' 
        AND {{ cs }}.call1_previous_goals_follow_up IN ('1_succeed', '2_tried') 
        AND {{ cs }}.call1_goals_sms IS NOT NULL AND {{ cs }}.call1_goals_sms != '' 
        THEN 1 ELSE NULL 
    END
{% endmacro %}

{% macro is_call0_ok_pm0_setup_call1_ok_pm0_ok_pm1_notsetup(cs) %}
    CASE 
        WHEN {{ cs }}.call0_status = 'OK' 
        AND {{ cs }}.call0_goals_sms IS NOT NULL AND {{ cs }}.call0_goals_sms != '' 
        AND {{ cs }}.call1_status = 'OK' 
        AND {{ cs }}.call1_previous_goals_follow_up IN ('1_succeed', '2_tried') 
        AND ({{ cs }}.call1_goals_sms IS NULL OR {{ cs }}.call1_goals_sms = '') 
        THEN 1 ELSE NULL 
    END
{% endmacro %}

-- Métriques pour call2 avec PM1 setup et PM0 OK
{% macro is_call0_ok_pm0_setup_call1_ok_pm0_ok_pm1_setup_call2_ok(cs) %}
    CASE 
        WHEN {{ cs }}.call0_status = 'OK' 
        AND {{ cs }}.call0_goals_sms IS NOT NULL AND {{ cs }}.call0_goals_sms != '' 
        AND {{ cs }}.call1_status = 'OK' AND {{ cs }}.call1_previous_goals_follow_up IN ('1_succeed', '2_tried') 
        AND {{ cs }}.call1_goals_sms IS NOT NULL AND {{ cs }}.call1_goals_sms != '' 
        AND {{ cs }}.call2_status = 'OK' 
        THEN 1 ELSE NULL 
    END
{% endmacro %}

{% macro is_call0_ok_pm0_setup_call1_ok_pm0_ok_pm1_setup_call2_ko(cs) %}
    CASE 
        WHEN {{ cs }}.call0_status = 'OK' 
        AND {{ cs }}.call0_goals_sms IS NOT NULL AND {{ cs }}.call0_goals_sms != '' 
        AND {{ cs }}.call1_status = 'OK' 
        AND {{ cs }}.call1_previous_goals_follow_up IN ('1_succeed', '2_tried') 
        AND {{ cs }}.call1_goals_sms IS NOT NULL AND {{ cs }}.call1_goals_sms != '' 
        AND {{ cs }}.call2_status != 'OK' 
        THEN 1 ELSE NULL 
    END
{% endmacro %}

-- Métriques pour PM1 follow-up avec call2 OK
{% macro is_call0_ok_call1_ok_pm0_ok_call2_ok_pm1_ok(cs) %}
    CASE 
        WHEN {{ cs }}.call0_status = 'OK' 
        AND {{ cs }}.call0_goals_sms IS NOT NULL AND {{ cs }}.call0_goals_sms != '' 
        AND {{ cs }}.call1_status = 'OK' 
        AND {{ cs }}.call1_previous_goals_follow_up IN ('1_succeed', '2_tried') 
        AND {{ cs }}.call1_goals_sms IS NOT NULL AND {{ cs }}.call1_goals_sms != '' 
        AND {{ cs }}.call2_status = 'OK' 
        AND {{ cs }}.call2_previous_goals_follow_up IN ('1_succeed', '2_tried') 
        THEN 1 ELSE NULL 
    END
{% endmacro %}

{% macro is_call0_ok_call1_ok_pm0_ok_call2_ok_pm1_ko(cs) %}
    CASE 
        WHEN {{ cs }}.call0_status = 'OK' 
        AND {{ cs }}.call0_goals_sms IS NOT NULL AND {{ cs }}.call0_goals_sms != '' 
        AND {{ cs }}.call1_status = 'OK' 
        AND {{ cs }}.call1_previous_goals_follow_up IN ('1_succeed', '2_tried') 
        AND {{ cs }}.call1_goals_sms IS NOT NULL AND {{ cs }}.call1_goals_sms != '' 
        AND {{ cs }}.call2_status = 'OK'
        AND {{ cs }}.call2_previous_goals_follow_up NOT IN ('1_succeed', '2_tried') 
        THEN 1 ELSE NULL 
    END
{% endmacro %}

-- Métriques pour PM1 setup avec PM0 KO
{% macro is_call0_ok_pm0_setup_call1_ok_pm0_ko_pm1_setup(cs) %}
    CASE 
        WHEN {{ cs }}.call0_status = 'OK' 
        AND {{ cs }}.call0_goals_sms IS NOT NULL AND {{ cs }}.call0_goals_sms != '' 
        AND {{ cs }}.call1_status = 'OK' 
        AND {{ cs }}.call1_previous_goals_follow_up NOT IN ('1_succeed', '2_tried') 
        AND {{ cs }}.call1_goals_sms IS NOT NULL AND {{ cs }}.call1_goals_sms != '' 
        THEN 1 ELSE NULL 
    END
{% endmacro %}

{% macro is_call0_ok_pm0_setup_call1_ok_pm0_ko_pm1_notsetup(cs) %}
    CASE 
        WHEN {{ cs }}.call0_status = 'OK' 
        AND {{ cs }}.call0_goals_sms IS NOT NULL AND {{ cs }}.call0_goals_sms != '' 
        AND {{ cs }}.call1_status = 'OK' 
        AND {{ cs }}.call1_previous_goals_follow_up NOT IN ('1_succeed', '2_tried') 
        AND ({{ cs }}.call1_goals_sms IS NULL OR {{ cs }}.call1_goals_sms = '') 
        THEN 1 ELSE NULL 
    END
{% endmacro %}

-- Métriques pour PM0 not setup
{% macro is_call0_ok_pm0_not_setup_call1_ok(cs) %}
    CASE 
        WHEN {{ cs }}.call0_status = 'OK' 
        AND ({{ cs }}.call0_goals_sms IS NULL OR {{ cs }}.call0_goals_sms = '') 
        AND {{ cs }}.call1_status = 'OK' 
        THEN 1 ELSE NULL 
    END
{% endmacro %}

{% macro is_call0_ok_pm0_not_setup_call1_ko(cs) %}
    CASE 
        WHEN {{ cs }}.call0_status = 'OK' 
        AND ({{ cs }}.call0_goals_sms IS NULL OR {{ cs }}.call0_goals_sms = '') 
        AND {{ cs }}.call1_status != 'OK' 
        THEN 1 ELSE NULL 
    END
{% endmacro %}

{% macro is_call0_ok_pm0_not_setup_call1_ok_pm1_setup(cs) %}
    CASE 
        WHEN {{ cs }}.call0_status = 'OK' 
        AND ({{ cs }}.call0_goals_sms IS NULL OR {{ cs }}.call0_goals_sms = '') 
        AND {{ cs }}.call1_status = 'OK' 
        AND {{ cs }}.call1_goals_sms IS NOT NULL AND {{ cs }}.call1_goals_sms != '' 
        THEN 1 ELSE NULL 
    END
{% endmacro %}

{% macro is_call0_ok_pm0_not_setup_call1_ok_pm1_notsetup(cs) %}
    CASE 
        WHEN {{ cs }}.call0_status = 'OK' 
        AND ({{ cs }}.call0_goals_sms IS NULL OR {{ cs }}.call0_goals_sms = '') 
        AND {{ cs }}.call1_status = 'OK' 
        AND ({{ cs }}.call1_goals_sms IS NULL OR {{ cs }}.call1_goals_sms = '') 
        THEN 1 ELSE NULL 
    END
{% endmacro %}

-- Métriques pour call0 KO
{% macro is_call0_ko_call1_ok(cs) %}
    CASE 
        WHEN {{ cs }}.call0_status!= 'OK' 
        AND {{ cs }}.call1_status = 'OK' 
        THEN 1 ELSE NULL 
    END
{% endmacro %}

{% macro is_call0_ko_call1_ko(cs) %}
    CASE 
        WHEN {{ cs }}.call0_status!= 'OK' 
        AND {{ cs }}.call1_status != 'OK' 
        THEN 1 ELSE NULL 
    END
{% endmacro %}

{% macro is_call0_ko_call1_ok_pm1_setup(cs) %}
    CASE 
        WHEN {{ cs }}.call0_status!= 'OK' 
        AND {{ cs }}.call1_status = 'OK' 
        AND {{ cs }}.call1_goals_sms IS NOT NULL AND {{ cs }}.call1_goals_sms != '' 
        THEN 1 ELSE NULL 
    END
{% endmacro %}

-- Métriques complexes pour PM0 KO et PM1 setup avec call2
{% macro is_call0_ok_pm0_setup_call1_ok_pm0_ko_pm1_setup_call2_ok(cs) %}
    CASE 
        WHEN {{ cs }}.call0_status = 'OK' -- APPEL 0 OK
        AND {{ cs }}.call0_goals_sms IS NOT NULL AND {{ cs }}.call0_goals_sms != '' -- PM0 SETUP
        AND {{ cs }}.call1_status = 'OK' -- CALL 1 OK
        AND {{ cs }}.call1_previous_goals_follow_up NOT IN ('1_succeed', '2_tried') -- PM0 ECHOUE
        AND {{ cs }}.call1_goals_sms IS NOT NULL AND {{ cs }}.call1_goals_sms != '' -- PM1 SETUP
        AND {{ cs }}.call2_status = 'OK' -- APPEL 2 OK 
        THEN 1 ELSE NULL 
    END
{% endmacro %}

{% macro is_call0_ok_pm0_setup_call1_ok_pm0_ko_pm1_setup_call2_ko(cs) %}
    CASE 
        WHEN {{ cs }}.call0_status = 'OK' -- APPEL 0 OK
        AND {{ cs }}.call0_goals_sms IS NOT NULL AND {{ cs }}.call0_goals_sms != '' -- PM0 SETUP
        AND {{ cs }}.call1_status = 'OK' -- CALL 1 OK
        AND {{ cs }}.call1_previous_goals_follow_up NOT IN ('1_succeed', '2_tried') -- PM0 ECHOUE
        AND {{ cs }}.call1_goals_sms IS NOT NULL AND {{ cs }}.call1_goals_sms != '' -- PM1 SETUP
        AND {{ cs }}.call2_status != 'OK' -- APPEL 2 KO 
        THEN 1 ELSE NULL 
    END
{% endmacro %}

{% macro is_pm0_setup_pm0_ko_pm1_setup_call2_ok_pm1_ok(cs) %}
    CASE 
        WHEN {{ cs }}.call0_status = 'OK' -- APPEL 0 OK
        AND {{ cs }}.call0_goals_sms IS NOT NULL AND {{ cs }}.call0_goals_sms != '' -- PM0 SETUP
        AND {{ cs }}.call1_status = 'OK' -- CALL 1 OK
        AND {{ cs }}.call1_previous_goals_follow_up NOT IN ('1_succeed', '2_tried') -- PM0 ECHOUE
        AND {{ cs }}.call1_goals_sms IS NOT NULL AND {{ cs }}.call1_goals_sms != '' -- PM1 SETUP
        AND {{ cs }}.call2_status = 'OK' -- APPEL 2 OK 
        AND {{ cs }}.call2_previous_goals_follow_up IN ('1_succeed', '2_tried') -- PM1 OK
        THEN 1 ELSE NULL 
    END
{% endmacro %}

{% macro is_pm0_setup_pm0_ko_pm1_setup_call2_ok_pm1_ko(cs) %}
    CASE 
        WHEN {{ cs }}.call0_status = 'OK' -- APPEL 0 OK
        AND {{ cs }}.call0_goals_sms IS NOT NULL AND {{ cs }}.call0_goals_sms != '' -- PM0 SETUP
        AND {{ cs }}.call1_status = 'OK' -- CALL 1 OK
        AND {{ cs }}.call1_previous_goals_follow_up NOT IN ('1_succeed', '2_tried') -- PM0 ECHOUE
        AND {{ cs }}.call1_goals_sms IS NOT NULL AND {{ cs }}.call1_goals_sms != '' -- PM1 SETUP
        AND {{ cs }}.call2_status = 'OK' -- APPEL 2 OK 
        AND {{ cs }}.call2_previous_goals_follow_up NOT IN ('1_succeed', '2_tried') -- PM1 ECHOUE
        THEN 1 ELSE NULL 
    END
{% endmacro %}

-- Métriques pour call0 KO avec call1 et call2
{% macro is_call0_ko_call1_ok_pm1_setup_call2_ok(cs) %}
    CASE 
        WHEN {{ cs }}.call0_status!= 'OK' -- APPEL 0 KO
        AND {{ cs }}.call1_status = 'OK' -- APPEL 1 OK
        AND {{ cs }}.call1_goals_sms IS NOT NULL AND {{ cs }}.call1_goals_sms != '' -- PM1 SETUP
        AND {{ cs }}.call2_status = 'OK' -- APPEL 2 OK 
        THEN 1 ELSE NULL 
    END
{% endmacro %}

{% macro is_call0_ko_call1_ok_pm1_setup_call2_ko(cs) %}
    CASE 
        WHEN {{ cs }}.call0_status!= 'OK' -- APPEL 0 KO
        AND {{ cs }}.call1_status = 'OK'  -- APPEL 1 OK
        AND {{ cs }}.call1_goals_sms IS NOT NULL AND {{ cs }}.call1_goals_sms != '' -- PM1 SETUP
        AND {{ cs }}.call2_status != 'OK' -- APPEL 2 KO 
        THEN 1 ELSE NULL 
    END
{% endmacro %}

{% macro is_call0_ko_call1_ok_pm1_setup_call2_ok_pm1_ok(cs) %}
    CASE 
        WHEN {{ cs }}.call0_status!= 'OK' -- APPEL 0 KO
        AND {{ cs }}.call1_status = 'OK'  -- APPEL 1 OK
        AND {{ cs }}.call1_goals_sms IS NOT NULL AND {{ cs }}.call1_goals_sms != '' -- PM1 SETUP
        AND {{ cs }}.call2_status = 'OK' -- APPEL 2 OK 
        AND {{ cs }}.call2_previous_goals_follow_up IN ('1_succeed', '2_tried') -- PM1 OK
        THEN 1 ELSE NULL 
    END
{% endmacro %}

{% macro is_call0_ko_call1_ok_pm1_setup_call2_ok_pm1_ko(cs) %}
    CASE 
        WHEN {{ cs }}.call0_status!= 'OK' -- APPEL 0 KO
        AND {{ cs }}.call1_status = 'OK' -- APPEL 1 OK
        AND {{ cs }}.call1_goals_sms IS NOT NULL AND {{ cs }}.call1_goals_sms != '' -- PM1 SETUP
        AND {{ cs }}.call2_status = 'OK' -- APPEL 2 OK 
        AND {{ cs }}.call2_previous_goals_follow_up NOT IN ('1_succeed', '2_tried') -- PM1 ECHOUE
        THEN 1 ELSE NULL 
    END
{% endmacro %}

-- Métriques pour PM0 not setup avec call1 et call2
{% macro is_call0_ok_call1_ok_pm1_setup_call2_ok(cs) %}
    CASE 
        WHEN {{ cs }}.call0_status = 'OK' -- APPEL 0 OK
        AND ({{ cs }}.call0_goals_sms IS NULL OR {{ cs }}.call0_goals_sms = '')  -- PM0 NOT SETUP
        AND {{ cs }}.call1_status = 'OK' -- APPEL 1 OK
        AND {{ cs }}.call1_goals_sms IS NOT NULL AND {{ cs }}.call1_goals_sms != '' -- PM1 SETUP
        AND {{ cs }}.call2_status = 'OK' -- APPEL 2 OK
        THEN 1 ELSE NULL 
    END
{% endmacro %}

{% macro is_call0_ok_call1_ok_pm1_setup_call2_ko(cs) %}
    CASE 
        WHEN {{ cs }}.call0_status = 'OK' -- APPEL 0 OK
        AND ({{ cs }}.call0_goals_sms IS NULL OR {{ cs }}.call0_goals_sms = '')  -- PM0 NOT SETUP
        AND {{ cs }}.call1_status = 'OK' -- APPEL 1 OK
        AND {{ cs }}.call1_goals_sms IS NOT NULL AND {{ cs }}.call1_goals_sms != '' -- PM1 SETUP
        AND {{ cs }}.call2_status != 'OK' -- APPEL 2 KO
        THEN 1 ELSE NULL 
    END
{% endmacro %}

{% macro is_call0_ok_call1_ok_pm1_setup_call2_ok_pm1_ok(cs) %}
    CASE 
        WHEN {{ cs }}.call0_status = 'OK' -- APPEL 0 OK
        AND ({{ cs }}.call0_goals_sms IS NULL OR {{ cs }}.call0_goals_sms = '') -- PM0 NOT SETUP
        AND {{ cs }}.call1_status = 'OK' -- APPEL 1 OK
        AND {{ cs }}.call1_goals_sms IS NOT NULL AND {{ cs }}.call1_goals_sms != '' -- PM1 SETUP
        AND {{ cs }}.call2_status = 'OK' -- APPEL 2 OK 
        AND {{ cs }}.call2_previous_goals_follow_up IN ('1_succeed', '2_tried') -- PM1 OK
        THEN 1 ELSE NULL 
    END
{% endmacro %}

{% macro is_call0_ok_call1_ok_pm1_setup_call2_ok_pm1_ko(cs) %}
    CASE 
        WHEN {{ cs }}.call0_status = 'OK' -- APPEL 0 OK
        AND ({{ cs }}.call0_goals_sms IS NULL OR {{ cs }}.call0_goals_sms = '') -- PM0 NOT SETUP
        AND {{ cs }}.call1_status = 'OK' -- APPEL 1 OK
        AND {{ cs }}.call1_goals_sms IS NOT NULL AND {{ cs }}.call1_goals_sms != '' -- PM1 SETUP
        AND {{ cs }}.call2_status = 'OK' -- APPEL 2 OK 
        AND {{ cs }}.call2_previous_goals_follow_up NOT IN ('1_succeed', '2_tried') -- PM1 KO
        THEN 1 ELSE NULL 
    END
{% endmacro %}

-- Dernière métrique
{% macro is_call0_ko_call1_ok_pm1_notsetup(cs) %}
    CASE 
        WHEN {{ cs }}.call0_status!= 'OK' 
        AND {{ cs }}.call1_status = 'OK' 
        AND ({{ cs }}.call1_goals_sms IS NULL OR {{ cs }}.call1_goals_sms = '') 
        THEN 1 ELSE NULL 
    END
{% endmacro %}
