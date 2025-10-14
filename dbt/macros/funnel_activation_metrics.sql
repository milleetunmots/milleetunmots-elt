
-- Métriques de base pour calls

{% macro clean_call_status(call_status) %}
    CASE
        when {{ call_status }} in ('OK', 'KO', 'Incomplet / Pas de choix de module', 'Ne pas appeler', 'Numéro erroné') then {{ call_status }}
        WHEN {{ call_status }} IS NOT NULL AND {{ call_status }} != '' then 'Autre'
        ELSE NULL
    END
{% endmacro %}

{% macro is_call_ok(call_status) %}
    CASE 
        WHEN {{ call_status }} = 'OK' 
        THEN 1 ELSE 0
    END
{% endmacro %}

{% macro is_call_ko(call_status) %}
    CASE 
        WHEN {{ call_status }} = 'KO' 
        THEN 1 ELSE 0 
    END
{% endmacro %}

{% macro is_call_not_ok(call_status) %}
    CASE 
        WHEN {{ call_status }} is null or {{ call_status }} != 'OK' 
        THEN 1 ELSE 0 
    END
{% endmacro %}

{% macro is_call_not_ko(call_status) %}
    CASE 
        WHEN {{ call_status }} is null or {{ call_status }} != 'KO' 
        THEN 1 ELSE 0 
    END
{% endmacro %}

{% macro is_pm_setup(call_goal_sms) %}
    CASE 
        WHEN {{ call_goal_sms }} IS NOT NULL AND {{ call_goal_sms }} != '' 
        THEN 'OUI' ELSE 'NON' 
    END
{% endmacro %}

{% macro pm_follow_up(call_previous_goals_follow_up) %}
    CASE {{ call_previous_goals_follow_up }}
        WHEN '1_succeed' then 'PM réussie'
        WHEN '2_tried' then 'PM essayée'
        WHEN '3_no_tried' then 'PM non essayée'
        WHEN '4_no_goal' then 'Pas de PM'
        when '5_not_enough_information' then 'Pas assez d\'information'
        ELSE NULL
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
