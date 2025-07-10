-- Test de comparaison entre l'ancien modèle fa.sql et le nouveau modèle funnel_activation.sql
-- Utilisation du package audit_helper de dbt

-- 1. Comparaison globale des deux modèles
{{ audit_helper.compare_queries(
    a_query=ref('fa'),
    b_query=ref('funnel_activation'),
    primary_key="id",
    summarize=true
) }}

-- 2. Comparaison détaillée avec audit_helper.compare_column_values
{{ audit_helper.compare_column_values(
    a_query=ref('fa'),
    b_query=ref('funnel_activation'),
    primary_key="id",
    column_to_compare="is_call0_ok"
) }}

{{ audit_helper.compare_column_values(
    a_query=ref('fa'),
    b_query=ref('funnel_activation'),
    primary_key="id",
    column_to_compare="is_call0_ko"
) }}

{{ audit_helper.compare_column_values(
    a_query=ref('fa'),
    b_query=ref('funnel_activation'),
    primary_key="id",
    column_to_compare="is_call0_ok_pm0_setup"
) }}

{{ audit_helper.compare_column_values(
    a_query=ref('fa'),
    b_query=ref('funnel_activation'),
    primary_key="id",
    column_to_compare="is_call0_ok_pm0_not_setup"
) }}

{{ audit_helper.compare_column_values(
    a_query=ref('fa'),
    b_query=ref('funnel_activation'),
    primary_key="id",
    column_to_compare="is_call0_ok_pm0_setup_call1_ok"
) }}

{{ audit_helper.compare_column_values(
    a_query=ref('fa'),
    b_query=ref('funnel_activation'),
    primary_key="id",
    column_to_compare="is_call0_ok_pm0_setup_call1_ko"
) }}

{{ audit_helper.compare_column_values(
    a_query=ref('fa'),
    b_query=ref('funnel_activation'),
    primary_key="id",
    column_to_compare="is_call0_ok_pm0_setup_call1_ko_pm0_ok"
) }}

{{ audit_helper.compare_column_values(
    a_query=ref('fa'),
    b_query=ref('funnel_activation'),
    primary_key="id",
    column_to_compare="is_call0_ok_pm0_setup_call1_ko_pm0_ko"
) }}

{{ audit_helper.compare_column_values(
    a_query=ref('fa'),
    b_query=ref('funnel_activation'),
    primary_key="id",
    column_to_compare="is_call0_ok_pm0_setup_call1_ok_pm0_ok"
) }}

{{ audit_helper.compare_column_values(
    a_query=ref('fa'),
    b_query=ref('funnel_activation'),
    primary_key="id",
    column_to_compare="is_call0_ok_pm0_setup_call1_ok_pm0_ko"
) }}

{{ audit_helper.compare_column_values(
    a_query=ref('fa'),
    b_query=ref('funnel_activation'),
    primary_key="id",
    column_to_compare="is_call0_ok_pm0_setup_call1_ok_pm0_ok_pm1_setup"
) }}

{{ audit_helper.compare_column_values(
    a_query=ref('fa'),
    b_query=ref('funnel_activation'),
    primary_key="id",
    column_to_compare="is_call0_ok_pm0_setup_call1_ok_pm0_ok_pm1_notsetup"
) }}

{{ audit_helper.compare_column_values(
    a_query=ref('fa'),
    b_query=ref('funnel_activation'),
    primary_key="id",
    column_to_compare="is_call0_ok_pm0_setup_call1_ok_pm0_ok_pm1_setup_call2_ok"
) }}

{{ audit_helper.compare_column_values(
    a_query=ref('fa'),
    b_query=ref('funnel_activation'),
    primary_key="id",
    column_to_compare="is_call0_ok_pm0_setup_call1_ok_pm0_ok_pm1_setup_call2_ko"
) }}

-- 3. Comparaison des colonnes de métadonnées
{{ audit_helper.compare_column_values(
    a_query=ref('fa'),
    b_query=ref('funnel_activation'),
    primary_key="id",
    column_to_compare="name"
) }}

{{ audit_helper.compare_column_values(
    a_query=ref('fa'),
    b_query=ref('funnel_activation'),
    primary_key="id",
    column_to_compare="parent1_postal_code"
) }}

{{ audit_helper.compare_column_values(
    a_query=ref('fa'),
    b_query=ref('funnel_activation'),
    primary_key="id",
    column_to_compare="source_channel"
) }}

{{ audit_helper.compare_column_values(
    a_query=ref('fa'),
    b_query=ref('funnel_activation'),
    primary_key="id",
    column_to_compare="tag_name"
) }}

-- 4. Test de cohérence des données avec audit_helper.get_column_values
-- Vérification que les valeurs sont cohérentes (pas de contradictions logiques)
WITH funnel_consistency_check AS (
    SELECT 
        id,
        -- Vérification que call0_ok et call0_ko sont mutuellement exclusifs
        CASE 
            WHEN is_call0_ok = 1 AND is_call0_ko = 1 THEN 'ERROR: call0_ok et call0_ko simultanés'
            ELSE 'OK'
        END as call0_consistency,
        
        -- Vérification que pm0_setup et pm0_not_setup sont mutuellement exclusifs
        CASE 
            WHEN is_call0_ok_pm0_setup = 1 AND is_call0_ok_pm0_not_setup = 1 THEN 'ERROR: pm0_setup et pm0_not_setup simultanés'
            ELSE 'OK'
        END as pm0_consistency,
        
        -- Vérification de la cohérence du funnel
        CASE 
            WHEN is_call0_ok_pm0_setup_call1_ok_pm0_ok_pm1_setup = 1 
                 AND (is_call0_ok != 1 OR is_call0_ok_pm0_setup != 1) 
            THEN 'ERROR: Incohérence dans le funnel pm1_setup'
            ELSE 'OK'
        END as funnel_consistency
        
    FROM {{ ref('funnel_activation') }}
    WHERE is_call0_ok = 1 OR is_call0_ko = 1
)

SELECT 
    'Funnel Consistency Check' as test_type,
    COUNT(*) as total_records,
    COUNT(CASE WHEN call0_consistency != 'OK' THEN 1 END) as call0_errors,
    COUNT(CASE WHEN pm0_consistency != 'OK' THEN 1 END) as pm0_errors,
    COUNT(CASE WHEN funnel_consistency != 'OK' THEN 1 END) as funnel_errors
FROM funnel_consistency_check

UNION ALL

-- 5. Résumé des différences par métrique
SELECT 
    'Summary by Metric' as test_type,
    COUNT(*) as total_records,
    SUM(CASE WHEN is_call0_ok = 1 THEN 1 ELSE 0 END) as call0_ok_count,
    SUM(CASE WHEN is_call0_ko = 1 THEN 1 ELSE 0 END) as call0_ko_count,
    SUM(CASE WHEN is_call0_ok_pm0_setup = 1 THEN 1 ELSE 0 END) as pm0_setup_count,
    SUM(CASE WHEN is_call0_ok_pm0_setup_call1_ok = 1 THEN 1 ELSE 0 END) as call1_ok_count,
    SUM(CASE WHEN is_call0_ok_pm0_setup_call1_ok_pm0_ok_pm1_setup = 1 THEN 1 ELSE 0 END) as pm1_setup_count,
    SUM(CASE WHEN is_call0_ok_pm0_setup_call1_ok_pm0_ok_pm1_setup_call2_ok = 1 THEN 1 ELSE 0 END) as call2_ok_count
FROM {{ ref('funnel_activation') }};
