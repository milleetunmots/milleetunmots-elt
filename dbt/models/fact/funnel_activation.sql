{{ config(materialized='table') }}

-- Sources avec refs isolées au début
WITH children AS (
    SELECT * FROM {{ ref('child') }}
),

parents AS (
    SELECT * FROM {{ ref('stg_1001mots_app__parents') }}
),

child_supports AS (
    SELECT * FROM {{ ref('stg_1001mots_app__child_supports') }}
),

admin_users AS (
    SELECT * FROM {{ ref('admin_users') }}
),

groups AS (
    SELECT * FROM {{ ref('groups') }}
),

children_sources AS (
    SELECT * FROM {{ ref('stg_1001mots_app__children_sources') }}
),

sources AS (
    SELECT * FROM {{ ref('stg_1001mots_app__sources') }}
),

taggings AS (
    SELECT * FROM {{ ref('stg_1001mots_app__taggings') }}
),

tags AS (
    SELECT * FROM {{ ref('stg_1001mots_app__tags') }}
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
        -- Tags des parents (taggable_type = 'Parent')
        array_agg(distinct cast(t1.tag_name as varchar)) as parent1_tags,
        array_agg(distinct cast(t2.tag_name as varchar)) as parent2_tags
    FROM children AS c
    LEFT JOIN parents AS p1 ON p1.parent_id = c.parent1_id 
    LEFT JOIN parents AS p2 ON p2.parent_id = c.parent2_id 
    LEFT JOIN child_supports AS cs ON c.family_id = cs.family_id
    LEFT JOIN admin_users AS au ON au.supporter_id = cs.supporter_id
    -- Jointures avec les tags des parents
    LEFT JOIN taggings AS tgs1
        ON tgs1.taggable_type = 'Parent'
        AND tgs1.taggable_id = p1.parent_id
    LEFT JOIN tags AS t1
        ON tgs1.tag_id = t1.tag_id
    LEFT JOIN taggings AS tgs2
        ON tgs2.taggable_type = 'Parent'
        AND tgs2.taggable_id = p2.parent_id
    LEFT JOIN tags AS t2
        ON tgs2.tag_id = t2.tag_id
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
        row_number() over (partition by ch.family_id order by ch.date_birth desc) as ranking,
        row_number() over (partition by ch.family_id order by ch.date_birth) as number_of_children
    FROM children AS ch
    -- On veut l'ensemble des enfants, même ceux qui ne sont pas dans un groupe ?
    LEFT JOIN groups AS g ON ch.group_id = g.group_id
    qualify ranking= 1
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
),

-- CTE pour récupérer les tags des familles (ChildSupport)
family_tags AS (
    SELECT 
        tg.taggable_id AS family_id,
        LISTAGG(t.tag_name, ',') WITHIN GROUP (ORDER BY t.tag_name) AS family_tag_list
    FROM taggings AS tg
    INNER JOIN tags AS t ON tg.tag_id = t.tag_id
    WHERE tg.taggable_type = 'ChildSupport'
    GROUP BY tg.taggable_id
)

SELECT DISTINCT
    cs.family_id as id,
    g.group_name as name,
    cpf.postal_code AS parent1_postal_code,
    s.channel AS source_channel,
    s.name AS source_name,
    -- Combinaison des tags des deux parents
    CASE 
        WHEN (cpf.parent1_tags IS NOT NULL AND ARRAY_SIZE(cpf.parent1_tags) > 0) 
             AND (cpf.parent2_tags IS NOT NULL AND ARRAY_SIZE(cpf.parent2_tags) > 0)
        THEN ARRAY_TO_STRING(cpf.parent1_tags, ',') || ',' || ARRAY_TO_STRING(cpf.parent2_tags, ',')
        WHEN cpf.parent1_tags IS NOT NULL AND ARRAY_SIZE(cpf.parent1_tags) > 0 
        THEN ARRAY_TO_STRING(cpf.parent1_tags, ',')
        WHEN cpf.parent2_tags IS NOT NULL AND ARRAY_SIZE(cpf.parent2_tags) > 0 
        THEN ARRAY_TO_STRING(cpf.parent2_tags, ',')
        ELSE NULL 
    END as tag_name,
    -- Tags des familles (ChildSupport)
    ft.family_tag_list,
    {{ clean_call_status('cs.call0_status') }} AS call0_status,
    {{ clean_call_status('cs.call1_status') }} AS call1_status,
    {{ clean_call_status('cs.call2_status') }} AS call2_status,
    {{ clean_call_status('cs.call3_status') }} AS call3_status,
    cs.call0_goals_sms,
    cs.call1_goals_sms,
    cs.call2_goals_sms,
    cs.call3_goals_sms,
    cs.call1_previous_goals_follow_up,
    cs.call2_previous_goals_follow_up,
    cs.call3_previous_goals_follow_up,
    
    -- New fields
    {{ is_call_ok('cs.call0_status') }} AS is_call0_ok,
    {{ is_call_ko('cs.call0_status') }} AS is_call0_ko,
    {{ is_call_not_ok('cs.call0_status') }} AS is_call0_not_ok,
    {{ is_call_not_ko('cs.call0_status') }} AS is_call0_not_ko,
    {{ is_call_ok('cs.call1_status') }} AS is_call1_ok,
    {{ is_call_ko('cs.call1_status') }} AS is_call1_ko,
    {{ is_call_not_ok('cs.call1_status') }} AS is_call1_not_ok,
    {{ is_call_not_ko('cs.call1_status') }} AS is_call1_not_ko,
    {{ is_call_ok('cs.call2_status') }} AS is_call2_ok,
    {{ is_call_ko('cs.call2_status') }} AS is_call2_ko,
    {{ is_call_not_ok('cs.call2_status') }} AS is_call2_not_ok,
    {{ is_call_not_ko('cs.call2_status') }} AS is_call2_not_ko,
    {{ is_call_ok('cs.call3_status') }} AS is_call3_ok,
    {{ is_call_ko('cs.call3_status') }} AS is_call3_ko,
    {{ is_call_not_ok('cs.call3_status') }} AS is_call3_not_ok,
    {{ is_call_not_ko('cs.call3_status') }} AS is_call3_not_ko,
    {{ is_pm_setup('cs.call0_goals_sms') }} AS is_pm0_setup,
    {{ is_pm_setup('cs.call1_goals_sms') }} AS is_pm1_setup,
    {{ is_pm_setup('cs.call2_goals_sms') }} AS is_pm2_setup,
    {{ is_pm_setup('cs.call3_goals_sms') }} AS is_pm3_setup,
    {{ pm_follow_up('cs.call1_previous_goals_follow_up') }} AS pm0_follow_up,
    {{ pm_follow_up('cs.call2_previous_goals_follow_up') }} AS pm1_follow_up,
    {{ pm_follow_up('cs.call3_previous_goals_follow_up') }} AS pm2_follow_up,

    -- Utilisation des macros pour les métriques de funnel
    
    {{ is_call0_ok_pm0_setup('cs') }} AS is_call0_ok_pm0_setup,
    {{ is_call0_ok_pm0_not_setup('cs') }} AS is_call0_ok_pm0_not_setup,
    {{ is_call0_ok_pm0_setup_call1_ok('cs') }} AS is_call0_ok_pm0_setup_call1_ok,
    {{ is_call0_ok_pm0_setup_call1_ko('cs') }} AS is_call0_ok_pm0_setup_call1_ko,
    {{ is_call0_ok_pm0_setup_call1_ko_pm0_ok('cs') }} AS is_call0_ok_pm0_setup_call1_ko_pm0_ok,
    {{ is_call0_ok_pm0_setup_call1_ko_pm0_ko('cs') }} AS is_call0_ok_pm0_setup_call1_ko_pm0_ko,
    {{ is_call0_ok_pm0_setup_call1_ok_pm0_ok('cs') }} AS is_call0_ok_pm0_setup_call1_ok_pm0_ok,
    {{ is_call0_ok_pm0_setup_call1_ok_pm0_ko('cs') }} AS is_call0_ok_pm0_setup_call1_ok_pm0_ko,
    {{ is_call0_ok_pm0_setup_call1_ok_pm0_ok_pm1_setup('cs') }} AS is_call0_ok_pm0_setup_call1_ok_pm0_ok_pm1_setup,
    {{ is_call0_ok_pm0_setup_call1_ok_pm0_ok_pm1_notsetup('cs') }} AS is_call0_ok_pm0_setup_call1_ok_pm0_ok_pm1_notsetup,
    {{ is_call0_ok_pm0_setup_call1_ok_pm0_ok_pm1_setup_call2_ok('cs') }} AS is_call0_ok_pm0_setup_call1_ok_pm0_ok_pm1_setup_call2_ok,
    {{ is_call0_ok_pm0_setup_call1_ok_pm0_ok_pm1_setup_call2_ko('cs') }} AS is_call0_ok_pm0_setup_call1_ok_pm0_ok_pm1_setup_call2_ko,
    {{ is_call0_ok_call1_ok_pm0_ok_call2_ok_pm1_ok('cs') }} AS is_call0_ok_call1_ok_pm0_ok_call2_ok_pm1_ok,
    {{ is_call0_ok_call1_ok_pm0_ok_call2_ok_pm1_ko('cs') }} AS is_call0_ok_call1_ok_pm0_ok_call2_ok_pm1_ko,
    {{ is_call0_ok_pm0_setup_call1_ok_pm0_ko_pm1_setup('cs') }} AS is_call0_ok_pm0_setup_call1_ok_pm0_ko_pm1_setup,
    {{ is_call0_ok_pm0_setup_call1_ok_pm0_ko_pm1_notsetup('cs') }} AS is_call0_ok_pm0_setup_call1_ok_pm0_ko_pm1_notsetup,
    {{ is_call0_ok_pm0_not_setup_call1_ok('cs') }} AS is_call0_ok_pm0_not_setup_call1_ok,
    {{ is_call0_ok_pm0_not_setup_call1_ko('cs') }} AS is_call0_ok_pm0_not_setup_call1_ko,
    {{ is_call0_ok_pm0_not_setup_call1_ok_pm1_setup('cs') }} AS is_call0_ok_pm0_not_setup_call1_ok_pm1_setup,
    {{ is_call0_ok_pm0_not_setup_call1_ok_pm1_notsetup('cs') }} AS is_call0_ok_pm0_not_setup_call1_ok_pm1_notsetup,
    {{ is_call0_ko_call1_ok('cs') }} AS is_call0_ko_call1_ok,
    {{ is_call0_ko_call1_ko('cs') }} AS is_call0_ko_call1_ko,
    {{ is_call0_ko_call1_ok_pm1_setup('cs') }} AS is_call0_ko_call1_ok_pm1_setup,
    {{ is_call0_ok_pm0_setup_call1_ok_pm0_ko_pm1_setup_call2_ok('cs') }} AS is_call0_ok_pm0_setup_call1_ok_pm0_ko_pm1_setup_call2_ok,
    {{ is_call0_ok_pm0_setup_call1_ok_pm0_ko_pm1_setup_call2_ko('cs') }} AS is_call0_ok_pm0_setup_call1_ok_pm0_ko_pm1_setup_call2_ko,
    {{ is_pm0_setup_pm0_ko_pm1_setup_call2_ok_pm1_ok('cs') }} AS is_pm0_setup_pm0_ko_pm1_setup_call2_ok_pm1_ok,
    {{ is_pm0_setup_pm0_ko_pm1_setup_call2_ok_pm1_ko('cs') }} AS is_pm0_setup_pm0_ko_pm1_setup_call2_ok_pm1_ko,
    {{ is_call0_ko_call1_ok_pm1_setup_call2_ok('cs') }} AS is_call0_ko_call1_ok_pm1_setup_call2_ok,
    {{ is_call0_ko_call1_ok_pm1_setup_call2_ko('cs') }} AS is_call0_ko_call1_ok_pm1_setup_call2_ko,
    {{ is_call0_ko_call1_ok_pm1_setup_call2_ok_pm1_ok('cs') }} AS is_call0_ko_call1_ok_pm1_setup_call2_ok_pm1_ok,
    {{ is_call0_ko_call1_ok_pm1_setup_call2_ok_pm1_ko('cs') }} AS is_call0_ko_call1_ok_pm1_setup_call2_ok_pm1_ko,
    {{ is_call0_ok_call1_ok_pm1_setup_call2_ok('cs') }} AS is_call0_ok_call1_ok_pm1_setup_call2_ok,
    {{ is_call0_ok_call1_ok_pm1_setup_call2_ko('cs') }} AS is_call0_ok_call1_ok_pm1_setup_call2_ko,
    {{ is_call0_ok_call1_ok_pm1_setup_call2_ok_pm1_ok('cs') }} AS is_call0_ok_call1_ok_pm1_setup_call2_ok_pm1_ok,
    {{ is_call0_ok_call1_ok_pm1_setup_call2_ok_pm1_ko('cs') }} AS is_call0_ok_call1_ok_pm1_setup_call2_ok_pm1_ko,
    {{ is_call0_ko_call1_ok_pm1_notsetup('cs') }} AS is_call0_ko_call1_ok_pm1_notsetup,

    -- Ajout d'une métriue d'engagement
    CASE 
        WHEN cs.call0_status = 'OK' AND cs.call1_status = 'OK' AND ((cs.call1_previous_goals_follow_up IN ('1_succeed', '2_tried')) OR (cs.call2_previous_goals_follow_up IN ('1_succeed', '2_tried'))) THEN 1
        WHEN cs.call0_status = 'OK' AND cs.call2_status = 'OK' AND ((cs.call1_previous_goals_follow_up IN ('1_succeed', '2_tried')) OR (cs.call2_previous_goals_follow_up IN ('1_succeed', '2_tried'))) THEN 1
        WHEN cs.call1_status = 'OK' AND cs.call2_status = 'OK' AND ((cs.call1_previous_goals_follow_up IN ('1_succeed', '2_tried')) OR (cs.call2_previous_goals_follow_up IN ('1_succeed', '2_tried'))) THEN 1
        ELSE 0 
    END AS deux_appels_ok_plus_une_pm

FROM child_parent_family AS cpf
INNER JOIN youngest_child AS yc ON yc.child_id = cpf.child_id
INNER JOIN child_supports AS cs ON cpf.family_id = cs.family_id
LEFT JOIN groups AS g ON yc.group_id = g.group_id
LEFT JOIN source AS s ON s.child_id = cpf.child_id
LEFT JOIN family_tags AS ft ON ft.family_id = cpf.family_id
