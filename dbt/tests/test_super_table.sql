-- Test de comparaison générale
{{ audit_helper.compare_queries(
    a_query=ref('st'),
    b_query=ref('super_table'),
    primary_key="family_id",
    summarize=true
) }}

-- Tests par colonne pour identifier précisément les différences
{% set columns_to_compare = [
    'family_id',
    'child_id', 
    'support_creation_date',
    'gender',
    'birthdate',
    'age_today_in_months',
    'age_range_at_start_of_cohort',
    'group_status',
    'end_of_active_status',
    'number_of_children',
    'is_desengage_T2',
    'is_estime_desengage_T2',
    'is_desengage_T1',
    'is_estime_desengage_T1_conserve',
    'is_estime_desengage_T1',
    'is_estime_desengage_T2_conserve',
    'engagement_state_t1',
    'engagement_state_t2',
    'parent1_city_name',
    'source_name',
    'source_channel',
    'source_department',
    'parent1_postal_code',
    'departement',
    'parent1_degree',
    'cohort_name',
    'supporter_name',
    'call_0_status',
    'call1_status',
    'call2_status',
    'call3_status',
    'call1_previous_goals_follow_up',
    'call2_previous_goals_follow_up',
    'call4_previous_goals_follow_up',
    'is_call0_goals',
    'is_call1_goals',
    'is_call2_goals',
    'is_call3_goals',
    'is_call0_status',
    'is_call1_status',
    'is_call2_status',
    'is_call3_status',
    'number_of_calls',
    'call_0_duration',
    'call1_duration',
    'call2_duration',
    'call3_duration',
    'review_call0',
    'review_call1',
    'review_call2',
    'review_call3',
    'nb_of_tries_call0',
    'nb_of_tries_call1',
    'nb_of_tries_call2',
    'nb_of_tries_call3',
    'mid_term_rate',
    'mid_term_reaction',
    'module2_name',
    'module3_name',
    'module4_name',
    'module5_name',
    'module6_name',
    'is_bilingue',
    'registration_delay',
    'age_at_registration',
    'tag_list',
    'is_excluded_from_analytics',
    'is_call_0_1_status_OK'
] %}

-- Tests de comparaison par colonne
{% for column in columns_to_compare %}
    -- Test pour la colonne {{ column }}
    {{ audit_helper.compare_column_values(
        a_query=ref('st'),
        b_query=ref('super_table'),
        primary_key="family_id",
        column_to_compare="{{ column }}"
    ) }}
{% endfor %}

-- Test de comparaison des colonnes critiques (statuts d'appels)
{% set critical_columns = [
    'call_0_status',
    'call1_status', 
    'call2_status',
    'call3_status',
    'is_call0_ok',
    'is_call1_ok',
    'is_call2_ok',
    'is_call3_ok'
] %}

{% for column in critical_columns %}
    -- Test critique pour {{ column }}
    {{ audit_helper.compare_column_values(
        a_query=ref('st'),
        b_query=ref('super_table'),
        primary_key="family_id",
        column_to_compare="{{ column }}"
    ) }}
{% endfor %}

-- Test de cohérence des données
WITH data_consistency_check AS (
    SELECT 
        family_id,
        -- Vérification que les statuts d'appels sont cohérents
        CASE 
            WHEN call_0_status = 'OK' AND call1_status = 'OK' AND call2_status = 'OK' AND call3_status = 'OK' THEN 'All calls OK'
            WHEN call_0_status = 'OK' AND call1_status = 'OK' AND call2_status = 'OK' THEN 'Calls 0-2 OK'
            WHEN call_0_status = 'OK' AND call1_status = 'OK' THEN 'Calls 0-1 OK'
            WHEN call_0_status = 'OK' THEN 'Only call 0 OK'
            ELSE 'No calls OK'
        END as call_sequence_status,
        
        -- Vérification de la cohérence des objectifs
        CASE 
            WHEN is_call0_goals = 1 AND is_call1_goals = 1 AND is_call2_goals = 1 AND is_call3_goals = 1 THEN 'All goals set'
            WHEN is_call0_goals = 1 AND is_call1_goals = 1 AND is_call2_goals = 1 THEN 'Goals 0-2 set'
            WHEN is_call0_goals = 1 AND is_call1_goals = 1 THEN 'Goals 0-1 set'
            WHEN is_call0_goals = 1 THEN 'Only goal 0 set'
            ELSE 'No goals set'
        END as goals_sequence_status
        
    FROM {{ ref('super_table') }}
)

SELECT 
    'Data Consistency Check' as test_type,
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
    COUNT(CASE WHEN goals_sequence_status = 'No goals set' THEN 1 END) as no_goals_set
FROM data_consistency_check;
