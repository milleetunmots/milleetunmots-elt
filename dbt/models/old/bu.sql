{{ config(materialized='table') }}

-- Sources avec source() isolées au début
WITH child_supports AS (
    SELECT * FROM {{ source('mots_app', 'child_supports') }}
),

children AS (
    SELECT * FROM {{ source('mots_app', 'children') }}
),

parents AS (
    SELECT * FROM {{ source('mots_app', 'parents') }}
),

groups AS (
    SELECT * FROM {{ source('mots_app', 'groups') }}
),

versions AS (
    SELECT * FROM {{ source('mots_app', 'versions') }}
),

-- CTEs métier
family AS (
    SELECT 
        id AS family_id,
        created_at,
        CASE 
            WHEN call0_status = 'OK' AND call1_status = 'OK' THEN 1
            WHEN call0_status = 'OK' AND call2_status = 'OK' THEN 1
            WHEN call0_status = 'OK' AND call3_status = 'OK' THEN 1
            WHEN call1_status = 'OK' AND call2_status = 'OK' THEN 1
            WHEN call3_status = 'OK' AND call1_status = 'OK' THEN 1
            WHEN call2_status = 'OK' AND call3_status = 'OK' THEN 1
            ELSE 0 END AS deux_appels_ok,
        
        -- PM essaye ou réussi call1_previous_goals_follow_up IN ('1_succeed', '2_tried') & call1_previous_goals_follow_up
        CASE 
            WHEN call0_status = 'OK' AND call1_status = 'OK' AND ((call1_previous_goals_follow_up IN ('1_succeed', '2_tried')) OR (call2_previous_goals_follow_up IN ('1_succeed', '2_tried'))) THEN 1
            WHEN call0_status = 'OK' AND call2_status = 'OK' AND ((call1_previous_goals_follow_up IN ('1_succeed', '2_tried')) OR (call2_previous_goals_follow_up IN ('1_succeed', '2_tried'))) THEN 1
            WHEN call1_status = 'OK' AND call2_status = 'OK' AND ((call1_previous_goals_follow_up IN ('1_succeed', '2_tried')) OR (call2_previous_goals_follow_up IN ('1_succeed', '2_tried'))) THEN 1
            ELSE 0 END AS deux_appels_ok_plus_une_pm,
        CASE WHEN call0_status = 'OK' THEN 1 ELSE 0 END AS is_call0_ok,
        CASE WHEN call1_status = 'OK' THEN 1 ELSE 0 END AS is_call1_ok,
        CASE WHEN call2_status = 'OK' THEN 1 ELSE 0 END AS is_call2_ok,
        CASE WHEN call3_status = 'OK' THEN 1 ELSE 0 END AS is_call3_ok,
        CASE WHEN call0_goals IS NULL THEN 0 WHEN call0_goals = '' THEN 0 ELSE 1 END AS is_call0_goals,
        CASE WHEN call1_goals IS NULL THEN 0 WHEN call1_goals = '' THEN 0 ELSE 1 END AS is_call1_goals,
        CASE WHEN call2_goals IS NULL THEN 0 WHEN call2_goals = '' THEN 0 ELSE 1 END AS is_call2_goals,
        CASE WHEN call3_goals IS NULL THEN 0 WHEN call3_goals = '' THEN 0 ELSE 1 END AS is_call3_goals,
        CASE WHEN call1_previous_goals_follow_up IN ('1_succeed', '2_tried') THEN 1 ELSE 0 END AS call1_previous_goals_done_or_tried,
        CASE WHEN call2_previous_goals_follow_up IN ('1_succeed', '2_tried') THEN 1 ELSE 0 END AS call2_previous_goals_done_or_tried,
        books_quantity,
        already_working_with,
        
        CASE
            WHEN call1_previous_goals_follow_up = '1_succeed' AND call2_previous_goals_follow_up = '1_succeed' THEN 2
            WHEN call1_previous_goals_follow_up = '1_succeed' AND call2_previous_goals_follow_up = '2_tried' THEN  2
            WHEN call1_previous_goals_follow_up = '2_tried' AND call2_previous_goals_follow_up = '1_succeed' THEN 2
            WHEN call1_previous_goals_follow_up = '2_tried' AND call2_previous_goals_follow_up = '2_tried' THEN 2 
            WHEN call1_previous_goals_follow_up = '1_succeed' THEN  1
            WHEN call2_previous_goals_follow_up = '2_tried' THEN 1
            WHEN call1_previous_goals_follow_up = '2_tried' THEN 1
            WHEN call2_previous_goals_follow_up = '1_succeed' THEN 1
            ELSE 0 END AS nb_pm_tried_or_succeed,
        module3_chosen_by_parents_id,
        module4_chosen_by_parents_id,
        CASE WHEN module3_chosen_by_parents_id IS NULL AND module4_chosen_by_parents_id IS NULL AND call3_status != 'OK' THEN 0 ELSE 1 END AS retention
        
    FROM child_supports
),

child AS (
    SELECT 
        child_support_id,
        g.name AS cohorte,
        ch.group_status,
        g.started_at,
        g.ended_at,
        COUNT(DISTINCT ch.id) AS nb_of_children,
        
        CASE 
            WHEN dsg.second_group_status = 'disengaged' THEN 'disengaged' 
            WHEN dsg.second_group_status = 'stopped' AND dsg.disengaged_at < g.ended_at THEN '2'
            WHEN dsg.second_group_status = 'stopped' AND dsg.disengaged_at > g.ended_at THEN NULL
            WHEN dsg.second_group_status = 'stopped' AND DATE_PART('day', dsg.disengaged_at::timestamp - g.started_at::timestamp) < 180 THEN '3'
            ELSE NULL END 
        AS is_child_disengaged,
        
        dsg.disengaged_at,
        SUBSTR(TRIM(p.postal_code), 1, 2) AS departement

    FROM children AS ch
    LEFT JOIN parents AS p ON p.id = ch.parent1_id
    LEFT JOIN groups AS g ON ch.group_id = g.id
    LEFT JOIN (
        SELECT
            item_type AS item_type,
            item_id AS item_id,
            object_changes->'group_status'->>1 AS second_group_status,
            date(object_changes->'updated_at'->>0) AS disengaged_at
        FROM versions
        WHERE object_changes->'group_status'->>1 IN ('disengaged', 'stopped')
    ) AS dsg ON dsg.item_id = ch.id

    GROUP BY 1,2,3,4,5,7,8,9
)

SELECT 
    f.*,
    c.cohorte,
    c.group_status,
    c.nb_of_children,
    c.started_at AS cohort_launch_date,
    c.ended_at AS cohort_end_date,
    c.is_child_disengaged,
    c.disengaged_at,
    c.departement
FROM family AS f 
LEFT JOIN child AS c ON f.family_id = c.child_support_id
