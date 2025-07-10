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
    {{ audit_helper.compare_queries(
        a_query=ref('bu'),
        b_query=ref('bureau'),
        primary_key="family_id",
        column_name="{{ column }}",
        summarize=true
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
    {{ audit_helper.compare_queries(
        a_query=ref('bu'),
        b_query=ref('bureau'),
        primary_key="family_id",
        column_name="{{ column }}",
        summarize=true
    ) }}
{% endfor %}
