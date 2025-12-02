{{ config(materialized='table') }}

-- Sources avec refs isolées au début
WITH aircall_calls AS (
    SELECT * FROM {{ ref('stg_1001mots_app__aircall_calls') }}
),

child_supports AS (
    SELECT * FROM {{ ref('stg_1001mots_app__child_supports') }}
),

dim_parent AS (
    SELECT * FROM {{ ref('parent') }}
),

dim_admin_users AS (
    SELECT * FROM {{ ref('admin_users') }}
),

-- CTE métier pour créer les lignes par appel (call 0 et call 1)
family AS (
    SELECT
        family_id AS id,
        0 AS numero_of_call,
        CASE 
            WHEN call0_status = 'OK' THEN 1 
            WHEN (call0_status IS NOT NULL AND call0_status != '') THEN 0 
            ELSE NULL 
        END AS is_call_ok,
        CASE 
            WHEN call0_goals_sms IS NOT NULL AND call0_goals_sms != '' THEN 1 
            ELSE 0 
        END AS is_pm_setup,
        'Pas de pm à checker' AS previous_call_pm_status,
        call0_review AS call_review
    FROM child_supports
    WHERE call0_status = 'OK' AND call1_status = 'OK'

    UNION ALL 

    SELECT 
        family_id AS id,
        1 AS numero_of_call,
        CASE 
            WHEN call1_status = 'OK' THEN 1 
            WHEN (call1_status IS NOT NULL AND call1_status != '') THEN 0 
            ELSE NULL 
        END AS is_call_ok,
        CASE 
            WHEN call1_goals_sms IS NOT NULL AND call1_goals_sms != '' THEN 1 
            ELSE 0 
        END AS is_pm_setup,
        call1_previous_goals_follow_up AS previous_call_pm_status,
        call1_review AS call_review
    FROM child_supports
    WHERE call0_status = 'OK' AND call1_status = 'OK'
)

SELECT 
    au.name AS supporter_name,
    ac.child_support_id,
    p.first_name,
    p.last_name,
    ac.started_at,
    ac.call_session,
    ac.duration,
    ac.asset_url,
    CAST(ac.tags AS TEXT) AS tags,
    f.is_pm_setup,
    f.previous_call_pm_status,
    f.call_review
FROM aircall_calls AS ac 
LEFT JOIN dim_parent AS p ON ac.parent_id = p.parent_id 
LEFT JOIN dim_admin_users AS au ON au.supporter_id = ac.caller_id
INNER JOIN family AS f ON ac.child_support_id = f.id AND ac.call_session = f.numero_of_call
ORDER BY 
    supporter_name,
    child_support_id, 
    call_session, 
    started_at

