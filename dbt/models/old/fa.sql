{{ config(materialized='table') }}

-- Sources avec source() isolées au début
WITH children AS (
    SELECT * FROM {{ source('mots_app', 'children') }}
),

parents AS (
    SELECT * FROM {{ source('mots_app', 'parents') }}
),

child_supports AS (
    SELECT * FROM {{ source('mots_app', 'child_supports') }}
),

admin_users AS (
    SELECT * FROM {{ source('mots_app', 'admin_users') }}
),

groups AS (
    SELECT * FROM {{ source('mots_app', 'groups') }}
),

children_sources AS (
    SELECT * FROM {{ source('mots_app', 'children_sources') }}
),

sources AS (
    SELECT * FROM {{ source('mots_app', 'sources') }}
),

taggings AS (
    SELECT * FROM {{ source('mots_app', 'taggings') }}
),

tags AS (
    SELECT * FROM {{ source('mots_app', 'tags') }}
),

-- CTEs métier
child_parent_family AS (
    SELECT 
        c.id AS child_id,
        p1.id AS parent1_id,
        p2.id AS parent2_id,
        cs.id AS family_id,
        au.id AS supporter_id,
        p1.postal_code,
        array_remove(array_agg(distinct cast(t1.name as varchar)) ||array_agg( distinct cast(t2.name as varchar)), NULL) as tag_name
    FROM children AS c
    LEFT JOIN parents AS p1 ON p1.id = c.parent1_id 
    LEFT JOIN parents AS p2 ON p2.id = c.parent2_id 
    LEFT JOIN child_supports AS cs ON c.child_support_id = cs.id
    LEFT JOIN admin_users AS au ON au.id = cs.supporter_id
    LEFT JOIN taggings AS tgs1
        ON tgs1.taggable_type = 'Parent'
        AND tgs1.taggable_id = p1.id
    LEFT JOIN tags AS t1
        ON tgs1.tag_id = t1.id
    LEFT JOIN taggings AS tgs2
        ON tgs2.taggable_type = 'Parent'
        AND tgs2.taggable_id = p2.id
    LEFT JOIN tags AS t2
        ON tgs2.tag_id = t2.id
    WHERE c.discarded_at IS NULL
    GROUP BY 1,2,3,4,5,6
),

youngest_child AS (
    SELECT 
        id AS child_id,
        group_id,
        gender, 
        birthdate,
        (DATE_PART('year', CURRENT_DATE) - DATE_PART('year',  birthdate)) * 12 + (DATE_PART('month', CURRENT_DATE) - DATE_PART('month', birthdate)) AS ages,
        registration_source,
        group_status,
        group_end,
        number_of_children
    FROM children AS ch
    INNER JOIN (
        SELECT
            child_support_id, 
            MAX(birthdate) AS youngest_child_birthdate,
            MAX(id) as youngest_child_id,
            COUNT(id) AS number_of_children
        FROM children
        GROUP BY child_support_id
    ) AS max_birthdate
    ON max_birthdate.child_support_id = ch.child_support_id AND max_birthdate.youngest_child_birthdate = ch.birthdate
),

source AS (
    SELECT 
        cs.child_id,
        cs.source_id,
        s.name, 
        s.channel, 
        s.department
    FROM children_sources AS cs
    INNER JOIN sources AS s ON cs.source_id = s.id
)

SELECT DISTINCT
    cs.id,
    g.name,
    cpf.postal_code AS parent1_postal_code,
    s.channel AS source_channel,
    array_to_string(ARRAY(SELECT DISTINCT UNNEST(cpf.tag_name::varchar[]) ORDER BY 1), ',') as tag_name,
    CASE 
        WHEN cs.call0_status = 'OK' 
        THEN 1 ELSE NULL END AS is_call0_ok,
   
    CASE 
        WHEN cs.call0_status != 'OK' 
        THEN 1 ELSE NULL END AS is_call0_ko,
    
    CASE 
        WHEN cs.call0_status = 'OK' 
        AND cs.call0_goals_sms IS NOT NULL AND cs.call0_goals_sms != '' 
        THEN 1 ELSE NULL END AS is_call0_ok_pm0_setup,
    
    CASE 
        WHEN cs.call0_status = 'OK' 
        AND (cs.call0_goals_sms IS NULL OR cs.call0_goals_sms = '') 
        THEN 1 ELSE NULL END AS is_call0_ok_pm0_not_setup,
    
    CASE 
        WHEN cs.call0_status = 'OK' 
        AND cs.call0_goals_sms IS NOT NULL AND cs.call0_goals_sms != '' 
        AND cs.call1_status = 'OK' 
        THEN 1 ELSE NULL END AS is_call0_ok_pm0_setup_call1_ok,
    
    CASE 
        WHEN cs.call0_status = 'OK' 
        AND cs.call0_goals IS NOT NULL AND cs.call0_goals_sms != '' 
        AND cs.call1_status != 'OK' 
        THEN 1 ELSE NULL END AS is_call0_ok_pm0_setup_call1_ko,
    
    CASE 
        WHEN cs.call0_status = 'OK' 
        AND cs.call0_goals_sms IS NOT NULL AND cs.call0_goals_sms != '' 
        AND cs.call1_status != 'OK' 
        AND cs.call1_previous_goals_follow_up IN ('1_succeed', '2_tried') 
        THEN 1 ELSE NULL END AS is_call0_ok_pm0_setup_call1_ko_pm0_ok,
   
   CASE 
        WHEN cs.call0_status = 'OK' 
        AND cs.call0_goals_sms IS NOT NULL AND cs.call0_goals_sms != '' 
        AND cs.call1_status != 'OK' 
        AND cs.call1_previous_goals_follow_up NOT IN ('1_succeed', '2_tried') 
        THEN 1 ELSE NULL END AS is_call0_ok_pm0_setup_call1_ko_pm0_ko,
        
    CASE 
        WHEN cs.call0_status = 'OK'
        AND cs.call0_goals_sms IS NOT NULL AND cs.call0_goals_sms != '' 
        AND cs.call1_status = 'OK' 
        AND cs.call1_previous_goals_follow_up IN ('1_succeed', '2_tried') 
        THEN 1 ELSE NULL END AS is_call0_ok_pm0_setup_call1_ok_pm0_ok,
    
    CASE
        WHEN cs.call0_status = 'OK' 
        AND cs.call0_goals_sms IS NOT NULL AND cs.call0_goals_sms != '' 
        AND cs.call1_status = 'OK' 
        AND cs.call1_previous_goals_follow_up NOT IN ('1_succeed', '2_tried') 
        THEN 1 ELSE NULL END AS is_call0_ok_pm0_setup_call1_ok_pm0_ko,
    
    CASE 
        WHEN cs.call0_status = 'OK' 
        AND cs.call0_goals_sms IS NOT NULL AND cs.call0_goals_sms != '' 
        AND cs.call1_status = 'OK' 
        AND cs.call1_previous_goals_follow_up IN ('1_succeed', '2_tried') 
        AND cs.call1_goals_sms IS NOT NULL AND cs.call1_goals_sms != '' 
        THEN 1 ELSE NULL END AS is_call0_ok_pm0_setup_call1_ok_pm0_ok_pm1_setup,
    
    CASE 
        WHEN cs.call0_status = 'OK' 
        AND cs.call0_goals_sms IS NOT NULL AND cs.call0_goals_sms != '' 
        AND cs.call1_status = 'OK' 
        AND cs.call1_previous_goals_follow_up IN ('1_succeed', '2_tried') 
        AND (cs.call1_goals_sms IS NULL OR cs.call1_goals_sms = '') 
        THEN 1 ELSE NULL END AS is_call0_ok_pm0_setup_call1_ok_pm0_ok_pm1_notsetup,
    
    CASE 
        WHEN cs.call0_status = 'OK' 
        AND cs.call0_goals_sms IS NOT NULL AND cs.call0_goals_sms != '' 
        AND cs.call1_status = 'OK' AND cs.call1_previous_goals_follow_up IN ('1_succeed', '2_tried') 
        AND cs.call1_goals_sms IS NOT NULL AND cs.call1_goals_sms != '' 
        AND cs.call2_status = 'OK' 
        THEN 1 ELSE NULL END AS is_call0_ok_pm0_setup_call1_ok_pm0_ok_pm1_setup_call2_ok,
    
    CASE 
        WHEN cs.call0_status = 'OK' 
        AND cs.call0_goals_sms IS NOT NULL AND cs.call0_goals_sms != '' 
        AND cs.call1_status = 'OK' 
        AND cs.call1_previous_goals_follow_up IN ('1_succeed', '2_tried') 
        AND cs.call1_goals_sms IS NOT NULL AND cs.call1_goals_sms != '' 
        AND cs.call2_status != 'OK' 
        THEN 1 ELSE NULL END AS is_call0_ok_pm0_setup_call1_ok_pm0_ok_pm1_setup_call2_ko,
    
    CASE 
        WHEN cs.call0_status = 'OK' 
        AND cs.call0_goals_sms IS NOT NULL AND cs.call0_goals_sms != '' 
        AND cs.call1_status = 'OK' 
        AND cs.call1_previous_goals_follow_up IN ('1_succeed', '2_tried') 
        AND cs.call1_goals_sms IS NOT NULL AND cs.call1_goals_sms != '' 
        AND cs.call2_status = 'OK' 
        AND cs.call2_previous_goals_follow_up IN ('1_succeed', '2_tried') 
        THEN 1 ELSE NULL END AS is_call0_ok_call1_ok_pm0_ok_call2_ok_pm1_ok,
    
    CASE 
        WHEN cs.call0_status = 'OK' 
        AND cs.call0_goals_sms IS NOT NULL AND cs.call0_goals_sms != '' 
        AND cs.call1_status = 'OK' 
        AND cs.call1_previous_goals_follow_up IN ('1_succeed', '2_tried') 
        AND cs.call1_goals_sms IS NOT NULL AND cs.call1_goals_sms != '' 
        AND cs.call2_status = 'OK'
        AND cs.call2_previous_goals_follow_up NOT IN ('1_succeed', '2_tried') 
        THEN 1 ELSE NULL END AS is_call0_ok_call1_ok_pm0_ok_call2_ok_pm1_ko,
    
    CASE 
        WHEN cs.call0_status = 'OK' 
        AND cs.call0_goals_sms IS NOT NULL AND cs.call0_goals_sms != '' 
        AND cs.call1_status = 'OK' 
        AND cs.call1_previous_goals_follow_up NOT IN ('1_succeed', '2_tried') 
        AND cs.call1_goals_sms IS NOT NULL AND cs.call1_goals_sms != '' 
        THEN 1 ELSE NULL END AS is_call0_ok_pm0_setup_call1_ok_pm0_ko_pm1_setup,
    
    CASE 
        WHEN cs.call0_status = 'OK' 
        AND cs.call0_goals_sms IS NOT NULL AND cs.call0_goals_sms != '' 
        AND cs.call1_status = 'OK' 
        AND cs.call1_previous_goals_follow_up NOT IN ('1_succeed', '2_tried') 
        AND (cs.call1_goals_sms IS NULL OR cs.call1_goals_sms = '') 
        THEN 1 ELSE NULL END AS is_call0_ok_pm0_setup_call1_ok_pm0_ko_pm1_notsetup,
    
    CASE 
        WHEN cs.call0_status = 'OK' 
        AND (cs.call0_goals_sms IS NULL OR cs.call0_goals_sms = '') 
        AND cs.call1_status = 'OK' 
        THEN 1 ELSE NULL END AS is_call0_ok_pm0_not_setup_call1_ok,
    
    CASE 
        WHEN cs.call0_status = 'OK' 
        AND (cs.call0_goals_sms IS NULL OR cs.call0_goals_sms = '') 
        AND cs.call1_status != 'OK' 
        THEN 1 ELSE NULL END AS is_call0_ok_pm0_not_setup_call1_ko,
    
    CASE 
        WHEN cs.call0_status = 'OK' 
        AND (cs.call0_goals_sms IS NULL OR cs.call0_goals_sms = '') 
        AND cs.call1_status = 'OK' 
        AND cs.call1_goals_sms IS NOT NULL AND cs.call1_goals_sms != '' 
        THEN 1 ELSE NULL END AS is_call0_ok_pm0_not_setup_call1_ok_pm1_setup,
    
    CASE 
        WHEN cs.call0_status = 'OK' 
        AND (cs.call0_goals_sms IS NULL OR cs.call0_goals_sms = '') 
        AND cs.call1_status = 'OK' 
        AND (cs.call1_goals_sms IS NULL OR cs.call1_goals_sms = '') 
        THEN 1 ELSE NULL END AS is_call0_ok_pm0_not_setup_call1_ok_pm1_notsetup,
    
    CASE 
        WHEN cs.call0_status!= 'OK' 
        AND cs.call1_status = 'OK' 
        THEN 1 ELSE NULL END AS is_call0_ko_call1_ok,
    
    CASE 
        WHEN cs.call0_status!= 'OK' 
        AND cs.call1_status != 'OK' 
        THEN 1 ELSE NULL END AS is_call0_ko_call1_ko,
    
    CASE 
        WHEN cs.call0_status!= 'OK' 
        AND cs.call1_status = 'OK' 
        AND cs.call1_goals_sms IS NOT NULL AND cs.call1_goals_sms != '' 
        THEN 1 ELSE NULL END AS is_call0_ko_call1_ok_pm1_setup,
 
    CASE 
        WHEN cs.call0_status = 'OK' -- APPEL 0 OK
        AND cs.call0_goals_sms IS NOT NULL AND cs.call0_goals_sms != '' -- PM0 SETUP
        AND cs.call1_status = 'OK' -- CALL 1 OK
        AND cs.call1_previous_goals_follow_up NOT IN ('1_succeed', '2_tried') -- PM0 ECHOUE
        AND cs.call1_goals_sms IS NOT NULL AND cs.call1_goals_sms != '' -- PM1 SETUP
        AND cs.call2_status = 'OK' -- APPEL 2 OK 
        THEN 1 ELSE NULL END AS is_call0_ok_pm0_setup_call1_ok_pm0_ko_pm1_setup_call2_ok,

    CASE 
        WHEN cs.call0_status = 'OK' -- APPEL 0 OK
        AND cs.call0_goals_sms IS NOT NULL AND cs.call0_goals_sms != '' -- PM0 SETUP
        AND cs.call1_status = 'OK' -- CALL 1 OK
        AND cs.call1_previous_goals_follow_up NOT IN ('1_succeed', '2_tried') -- PM0 ECHOUE
        AND cs.call1_goals_sms IS NOT NULL AND cs.call1_goals_sms != '' -- PM1 SETUP
        AND cs.call2_status != 'OK' -- APPEL 2 KO 
        THEN 1 ELSE NULL END AS is_call0_ok_pm0_setup_call1_ok_pm0_ko_pm1_setup_call2_ko,
    
    CASE 
        WHEN cs.call0_status = 'OK' -- APPEL 0 OK
        AND cs.call0_goals_sms IS NOT NULL AND cs.call0_goals_sms != '' -- PM0 SETUP
        AND cs.call1_status = 'OK' -- CALL 1 OK
        AND cs.call1_previous_goals_follow_up NOT IN ('1_succeed', '2_tried') -- PM0 ECHOUE
        AND cs.call1_goals_sms IS NOT NULL AND cs.call1_goals_sms != '' -- PM1 SETUP
        AND cs.call2_status = 'OK' -- APPEL 2 OK 
        AND cs.call2_previous_goals_follow_up IN ('1_succeed', '2_tried') -- PM1 OK
        THEN 1 ELSE NULL END AS is_pm0_setup_pm0_ko_pm1_setup_call2_ok_pm1_ok,
    
   CASE 
        WHEN cs.call0_status = 'OK' -- APPEL 0 OK
        AND cs.call0_goals_sms IS NOT NULL AND cs.call0_goals_sms != '' -- PM0 SETUP
        AND cs.call1_status = 'OK' -- CALL 1 OK
        AND cs.call1_previous_goals_follow_up NOT IN ('1_succeed', '2_tried') -- PM0 ECHOUE
        AND cs.call1_goals_sms IS NOT NULL AND cs.call1_goals_sms != '' -- PM1 SETUP
        AND cs.call2_status = 'OK' -- APPEL 2 OK 
        AND cs.call2_previous_goals_follow_up NOT IN ('1_succeed', '2_tried') -- PM1 ECHOUE
        THEN 1 ELSE NULL END AS is_pm0_setup_pm0_ko_pm1_setup_call2_ok_pm1_ko,
    
    CASE 
        WHEN cs.call0_status!= 'OK' -- APPEL 0 KO
        AND cs.call1_status = 'OK' -- APPEL 1 OK
        AND cs.call1_goals_sms IS NOT NULL AND cs.call1_goals_sms != '' -- PM1 SETUP
        AND cs.call2_status = 'OK' -- APPEL 2 OK 
        THEN 1 ELSE NULL END AS is_call0_ko_call1_ok_pm1_setup_call2_ok,
    
    CASE 
        WHEN cs.call0_status!= 'OK' -- APPEL 0 KO
        AND cs.call1_status = 'OK'  -- APPEL 1 OK
        AND cs.call1_goals_sms IS NOT NULL AND cs.call1_goals_sms != '' -- PM1 SETUP
        AND cs.call2_status != 'OK' -- APPEL 2 KO 
        THEN 1 ELSE NULL END AS is_call0_ko_call1_ok_pm1_setup_call2_ko,
    
    CASE 
        WHEN cs.call0_status!= 'OK' -- APPEL 0 KO
        AND cs.call1_status = 'OK'  -- APPEL 1 OK
        AND cs.call1_goals_sms IS NOT NULL AND cs.call1_goals_sms != '' -- PM1 SETUP
        AND cs.call2_status = 'OK' -- APPEL 2 OK 
        AND cs.call2_previous_goals_follow_up IN ('1_succeed', '2_tried') -- PM1 OK
        THEN 1 ELSE NULL END AS is_call0_ko_call1_ok_pm1_setup_call2_ok_pm1_ok,
    
    CASE 
        WHEN cs.call0_status!= 'OK' -- APPEL 0 KO
        AND cs.call1_status = 'OK' -- APPEL 1 OK
        AND cs.call1_goals_sms IS NOT NULL AND cs.call1_goals_sms != '' -- PM1 SETUP
        AND cs.call2_status = 'OK' -- APPEL 2 OK 
        AND cs.call2_previous_goals_follow_up NOT IN ('1_succeed', '2_tried') -- PM1 ECHOUE
        THEN 1 ELSE NULL END AS is_call0_ko_call1_ok_pm1_setup_call2_ok_pm1_ko,
    
    CASE 
        WHEN cs.call0_status = 'OK' -- APPEL 0 OK
        AND (cs.call0_goals_sms IS NULL OR cs.call0_goals_sms = '')  -- PM0 NOT SETUP
        AND cs.call1_status = 'OK' -- APPEL 1 OK
        AND cs.call1_goals_sms IS NOT NULL AND cs.call1_goals_sms != '' -- PM1 SETUP
        AND cs.call2_status = 'OK' -- APPEL 2 OK
        THEN 1 ELSE NULL END AS is_call0_ok_call1_ok_pm1_setup_call2_ok,
    
    CASE 
        WHEN cs.call0_status = 'OK' -- APPEL 0 OK
        AND (cs.call0_goals_sms IS NULL OR cs.call0_goals_sms = '')  -- PM0 NOT SETUP
        AND cs.call1_status = 'OK' -- APPEL 1 OK
        AND cs.call1_goals_sms IS NOT NULL AND cs.call1_goals_sms != '' -- PM1 SETUP
        AND cs.call2_status != 'OK' -- APPEL 2 KO
        THEN 1 ELSE NULL END AS is_call0_ok_call1_ok_pm1_setup_call2_ko,
    
    CASE 
        WHEN cs.call0_status = 'OK' -- APPEL 0 OK
        AND (cs.call0_goals_sms IS NULL OR cs.call0_goals_sms = '') -- PM0 NOT SETUP
        AND cs.call1_status = 'OK' -- APPEL 1 OK
        AND cs.call1_goals_sms IS NOT NULL AND cs.call1_goals_sms != '' -- PM1 SETUP
        AND cs.call2_status = 'OK' -- APPEL 2 OK 
        AND cs.call2_previous_goals_follow_up IN ('1_succeed', '2_tried') -- PM1 OK
        THEN 1 ELSE NULL END AS is_call0_ok_call1_ok_pm1_setup_call2_ok_pm1_ok,
    
    CASE 
        WHEN cs.call0_status = 'OK' -- APPEL 0 OK
        AND (cs.call0_goals_sms IS NULL OR cs.call0_goals_sms = '') -- PM0 NOT SETUP
        AND cs.call1_status = 'OK' -- APPEL 1 OK
        AND cs.call1_goals_sms IS NOT NULL AND cs.call1_goals_sms != '' -- PM1 SETUP
        AND cs.call2_status = 'OK' -- APPEL 2 OK 
        AND cs.call2_previous_goals_follow_up NOT IN ('1_succeed', '2_tried') -- PM1 KO
        THEN 1 ELSE NULL END AS is_call0_ok_call1_ok_pm1_setup_call2_ok_pm1_ko,
 
    CASE 
        WHEN cs.call0_status!= 'OK' 
        AND cs.call1_status = 'OK' 
        AND (cs.call1_goals_sms IS NULL OR cs.call1_goals_sms = '') 
        THEN 1 ELSE NULL END AS is_call0_ko_call1_ok_pm1_notsetup

FROM child_parent_family AS cpf
INNER JOIN youngest_child AS yc ON yc.child_id = cpf.child_id
INNER JOIN child_supports AS cs ON cpf.family_id = cs.id
LEFT JOIN groups AS g ON yc.group_id = g.id
LEFT JOIN source AS s ON s.child_id = cpf.child_id
