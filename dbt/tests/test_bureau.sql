-- Test de comparaison générale
{{ audit_helper.compare_queries(
    a_query=ref('bu'),
    b_query=ref('bureau'),
    primary_key="family_id",
    summarize=true
) }}

-- Tests par colonne pour identifier précisément les différences
{% set columns_to_compare = [
    'family_id',
    'created_at',
    'deux_appels_ok',
    'deux_appels_ok_plus_une_pm',
    'is_call0_ok',
    'is_call1_ok',
    'is_call2_ok',
    'is_call3_ok',
    'is_call0_goals',
    'is_call1_goals',
    'is_call2_goals',
    'is_call3_goals',
    'call1_previous_goals_done_or_tried',
    'call2_previous_goals_done_or_tried',
    'books_quantity',
    'already_working_with',
    'nb_pm_tried_or_succeed',
    'module3_chosen_by_parents_id',
    'module4_chosen_by_parents_id',
    'retention',
    'cohorte',
    'group_status',
    'nb_of_children',
    'cohort_launch_date',
    'cohort_end_date',
    'is_child_disengaged',
    'disengaged_at',
    'departement'
] %}

-- Tests de comparaison par colonne
{% for column in columns_to_compare %}
    -- Test pour la colonne {{ column }}
    {{ audit_helper.compare_column_values(
        a_query=ref('bu'),
        b_query=ref('bureau'),
        primary_key="family_id",
        column_to_compare="{{ column }}"
    ) }}
{% endfor %}

-- Test de comparaison des colonnes critiques (statuts d'appels et PM)
{% set critical_columns = [
    'is_call0_ok',
    'is_call1_ok',
    'is_call2_ok',
    'is_call3_ok',
    'is_call0_goals',
    'is_call1_goals',
    'is_call2_goals',
    'is_call3_goals',
    'call1_previous_goals_done_or_tried',
    'call2_previous_goals_done_or_tried',
    'deux_appels_ok',
    'deux_appels_ok_plus_une_pm',
    'nb_pm_tried_or_succeed',
    'retention'
] %}

{% for column in critical_columns %}
    -- Test critique pour {{ column }}
    {{ audit_helper.compare_column_values(
        a_query=ref('bu'),
        b_query=ref('bureau'),
        primary_key="family_id",
        column_to_compare="{{ column }}"
    ) }}
{% endfor %}

-- Test de cohérence des données
WITH bureau_consistency_check AS (
    SELECT 
        family_id,
        -- Vérification de la cohérence des appels
        CASE 
            WHEN is_call0_ok = 1 AND is_call1_ok = 1 AND is_call2_ok = 1 AND is_call3_ok = 1 THEN 'All calls OK'
            WHEN is_call0_ok = 1 AND is_call1_ok = 1 AND is_call2_ok = 1 THEN 'Calls 0-2 OK'
            WHEN is_call0_ok = 1 AND is_call1_ok = 1 THEN 'Calls 0-1 OK'
            WHEN is_call0_ok = 1 THEN 'Only call 0 OK'
            ELSE 'No calls OK'
        END as call_sequence_status,
        
        -- Vérification de la cohérence des objectifs
        CASE 
            WHEN is_call0_goals = 1 AND is_call1_goals = 1 AND is_call2_goals = 1 AND is_call3_goals = 1 THEN 'All goals set'
            WHEN is_call0_goals = 1 AND is_call1_goals = 1 AND is_call2_goals = 1 THEN 'Goals 0-2 set'
            WHEN is_call0_goals = 1 AND is_call1_goals = 1 THEN 'Goals 0-1 set'
            WHEN is_call0_goals = 1 THEN 'Only goal 0 set'
            ELSE 'No goals set'
        END as goals_sequence_status,
        
        -- Vérification de la cohérence des métriques composites
        CASE 
            WHEN deux_appels_ok = 1 AND (is_call0_ok = 1 AND is_call1_ok = 1) THEN 'Consistent'
            WHEN deux_appels_ok = 0 AND (is_call0_ok = 0 OR is_call1_ok = 0) THEN 'Consistent'
            ELSE 'Inconsistent'
        END as deux_appels_consistency,
        
        CASE 
            WHEN deux_appels_ok_plus_une_pm = 1 AND deux_appels_ok = 1 AND nb_pm_tried_or_succeed >= 1 THEN 'Consistent'
            WHEN deux_appels_ok_plus_une_pm = 0 AND (deux_appels_ok = 0 OR nb_pm_tried_or_succeed = 0) THEN 'Consistent'
            ELSE 'Inconsistent'
        END as deux_appels_plus_pm_consistency
        
    FROM {{ ref('bureau') }}
)

SELECT 
    'Bureau Data Consistency Check' as test_type,
    COUNT(*) as total_records,
    COUNT(CASE WHEN call_sequence_status = 'All calls OK' THEN 1 END) as all_calls_ok,
    COUNT(CASE WHEN call_sequence_status = 'Calls 0-2 OK' THEN 1 END) as calls_0_2_ok,
    COUNT(CASE WHEN call_sequence_status = 'Calls 0-1 OK' THEN 1 END) as calls_0_1_ok,
    COUNT(CASE WHEN call_sequence_status = 'Only call 0 OK' THEN 1 END) as only_call0_ok,
    COUNT(CASE WHEN call_sequence_status = 'No calls OK' THEN 1 END) as no_calls_ok,
    COUNT(CASE WHEN goals_sequence_status = 'All goals set' THEN 1 END) as all_goals_set,
    COUNT(CASE WHEN goals_sequence_status = 'Goals 0-2 set' THEN 1 END) as goals_0_2_set,
    COUNT(CASE WHEN goals_sequence_status = 'Goals 0-1 set' THEN 1 END) as goals_0_1_set,
    COUNT(CASE WHEN goals_sequence_status = 'Only goal 0 set' THEN 1 END) as only_goal0_set,
    COUNT(CASE WHEN goals_sequence_status = 'No goals set' THEN 1 END) as no_goals_set,
    COUNT(CASE WHEN deux_appels_consistency = 'Consistent' THEN 1 END) as deux_appels_consistent,
    COUNT(CASE WHEN deux_appels_consistency = 'Inconsistent' THEN 1 END) as deux_appels_inconsistent,
    COUNT(CASE WHEN deux_appels_plus_pm_consistency = 'Consistent' THEN 1 END) as deux_appels_plus_pm_consistent,
    COUNT(CASE WHEN deux_appels_plus_pm_consistency = 'Inconsistent' THEN 1 END) as deux_appels_plus_pm_inconsistent
FROM bureau_consistency_check;
