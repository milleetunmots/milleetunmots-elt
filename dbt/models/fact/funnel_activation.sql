{{ config(materialized='table') }}

-- Sources avec refs isolées au début
WITH children AS (
    SELECT * FROM {{ ref('stg_1001mots_app__children') }}
),

parents AS (
    SELECT * FROM {{ ref('stg_1001mots_app__parents') }}
),

child_supports AS (
    SELECT * FROM {{ ref('stg_1001mots_app__child_supports') }}
),

admin_users AS (
    SELECT * FROM {{ ref('stg_1001mots_app__admin_users') }}
),

groups AS (
    SELECT * FROM {{ ref('stg_1001mots_app__groups') }}
),

children_sources AS (
    SELECT * FROM {{ ref('stg_1001mots_app__children_sources') }}
),

sources AS (
    SELECT * FROM {{ ref('stg_1001mots_app__sources') }}
),

-- CTEs métier
child_parent_family AS (
    SELECT 
        c.child_id,
        p1.parent_id AS parent1_id,
        p2.parent_id AS parent2_id,
        c.family_id,
        au.supporter_id,
        p1.postal_code,
        -- Note: Les tags ne sont pas disponibles dans les tables staging actuelles
        -- array_remove(array_agg(distinct cast(t1.name as varchar)) ||array_agg( distinct cast(t2.name as varchar)), NULL) as tag_name
        NULL as tag_name
    FROM children AS c
    LEFT JOIN parents AS p1 ON p1.parent_id = c.parent1_id 
    LEFT JOIN parents AS p2 ON p2.parent_id = c.parent2_id 
    LEFT JOIN child_supports AS cs ON c.family_id = cs.family_id
    LEFT JOIN admin_users AS au ON au.supporter_id = cs.supporter_id
    -- Note: Les jointures avec tags et taggings ne sont pas disponibles dans les tables staging actuelles
    -- left join taggings as tgs1
    --     on tgs1.taggable_type = 'Parent'
    --     and tgs1.taggable_id = parent1_id
    -- left join tags as t1
    --     on tgs1.tag_id = t1.id
    -- left join taggings as tgs2
    --     on tgs2.taggable_type = 'Parent'
    --     and tgs2.taggable_id = parent2_id
    -- left join tags as t2
    --     on tgs2.tag_id = t2.id
    WHERE c.date_discarded IS NULL
    GROUP BY 1,2,3,4,5,6
),

youngest_child AS (
    SELECT 
        ch.child_id,
        ch.group_id,
        ch.gender, 
        ch.date_birth as birthdate,
        (DATE_PART('year', CURRENT_DATE) - DATE_PART('year', ch.date_birth)) * 12 + (DATE_PART('month', CURRENT_DATE) - DATE_PART('month', ch.date_birth)) AS ages,
        ch.registration_source,
        ch.group_status,
        g.date_ended as group_end,
        max_birthdate.number_of_children
    FROM children AS ch
    INNER JOIN groups AS g ON ch.group_id = g.group_id
    INNER JOIN (
        SELECT
            family_id, 
            MAX(date_birth) AS youngest_child_birthdate,
            MAX(child_id) as youngest_child_id,
            COUNT(child_id) AS number_of_children
        FROM children
        GROUP BY family_id
    ) AS max_birthdate
    ON max_birthdate.family_id = ch.family_id AND max_birthdate.youngest_child_birthdate = ch.date_birth
),

source AS (
    SELECT 
        cs.child_id,
        cs.source_id,
        s.name, 
        s.channel, 
        s.department
    FROM children_sources AS cs
    INNER JOIN sources AS s ON cs.source_id = s.source_id
)

SELECT DISTINCT
    cs.family_id as id,
    g.group_name as name,
    cpf.postal_code AS parent1_postal_code,
    s.channel AS source_channel,
    -- Note: Les tags ne sont pas disponibles dans les tables staging actuelles
    -- array_to_string(ARRAY(SELECT DISTINCT UNNEST(cpf.tag_name::varchar[]) ORDER BY 1), ',') as tag_name,
    cpf.tag_name,
    
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
INNER JOIN child_supports AS cs ON cpf.family_id = cs.family_id
LEFT JOIN groups AS g ON yc.group_id = g.group_id
LEFT JOIN source AS s ON s.child_id = cpf.child_id