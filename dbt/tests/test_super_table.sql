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
    {{ audit_helper.compare_queries(
        a_query=ref('st'),
        b_query=ref('super_table'),
        primary_key="family_id",
        column_name="{{ column }}",
        summarize=true
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
    {{ audit_helper.compare_queries(
        a_query=ref('st'),
        b_query=ref('super_table'),
        primary_key="family_id",
        column_name="{{ column }}",
        summarize=true
    ) }}
{% endfor %}
