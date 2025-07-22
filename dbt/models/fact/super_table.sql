-- Sources avec refs isolées au début
WITH children AS (
    SELECT * FROM {{ ref('child') }}
),

parents AS (
    SELECT * FROM {{ ref('parent') }}
),

child_supports AS (
    SELECT * FROM {{ ref('stg_1001mots_app__child_supports') }}
),

admin_users AS (
    SELECT * FROM {{ ref('stg_1001mots_app__admin_users') }}
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

support_modules AS (
    SELECT * FROM {{ ref('stg_1001mots_app__support_modules') }}
),

-- CTE faisant la relation entre les parents, les enfants et la famille
child_parent_family AS (
    SELECT 
        c.child_id,
        p1.parent_id AS parent1_id, 
        p2.parent_id AS parent2_id,
        c.family_id,
        au.supporter_id
    FROM children AS c
    LEFT JOIN parents AS p1 ON p1.parent_id = c.parent1_id 
    LEFT JOIN parents AS p2 ON p2.parent_id = c.parent2_id 
    LEFT JOIN child_supports AS cs ON c.family_id = cs.family_id
    LEFT JOIN admin_users AS au ON au.supporter_id = cs.supporter_id
    WHERE c.date_discarded IS NULL
),

-- CTE permettant de ne garder que l'enfant principal de la famille défini comme étant le plus jeune et des attributs de cet enfant
youngest_child AS (
    SELECT 
        ch.child_id,
        ch.group_id,
        ch.gender, 
        ch.date_birth as birthdate,
        ch.ages,
        ch.registration_source,
        ch.group_status,
        --Idee ?
        --g.date_ended_clean as group_end,
        ch.date_group_end as group_end,
        max_birthdate.number_of_children
    FROM children AS ch
    INNER JOIN (
        SELECT
            family_id, 
            MAX(date_birth) AS youngest_child_birthdate,
            MAX(child_id) as youngest_child_id,
            COUNT(DISTINCT child_id) AS number_of_children
        FROM children
        GROUP BY family_id
    ) AS max_birthdate
    ON max_birthdate.family_id = ch.family_id AND max_birthdate.youngest_child_birthdate = ch.date_birth
),

-- CTE permettant de récupérer des infos sur les parents
parent AS (
    SELECT 
        parent_id,
        city_name, 
        TRIM(postal_code) AS postal_code,
        degree, 
        mid_term_rate,
        mid_term_reaction
    FROM parents
),

-- CTE permettant de récupérer des infos sur les familles, notamment sur les statuts des appels, les goals et leur atteinte
families AS (
    SELECT 
        family_id,
        date_created as created_at,
        call0_status,
        call1_status,
        call2_status,
        call3_status,
        {{ get_goals_follow_up('call1_previous_goals_follow_up') }} AS call1_previous_goals_follow_up,
        {{ get_goals_follow_up('call2_previous_goals_follow_up') }} AS call2_previous_goals_follow_up,
        {{ get_goals_follow_up('call4_previous_goals_follow_up') }} AS call4_previous_goals_follow_up,
        {{ get_boolean_flag('call0_goals_sms') }} AS is_call0_goals,
        {{ get_boolean_flag('call1_goals_sms') }} AS is_call1_goals,
        {{ get_boolean_flag('call2_goals_sms') }} AS is_call2_goals,
        {{ get_boolean_flag('call3_goals_sms') }} AS is_call3_goals,
        {{ get_boolean_flag('call0_status') }} AS is_call0_status,
        {{ get_boolean_flag('call1_status') }} AS is_call1_status,
        {{ get_boolean_flag('call2_status') }} AS is_call2_status,
        {{ get_boolean_flag('call3_status') }} AS is_call3_status,
        call0_duration,
        call1_duration,
        call2_duration,
        call3_duration,
        module2_chosen_by_parents_id,
        module3_chosen_by_parents_id,
        module4_chosen_by_parents_id,
        module5_chosen_by_parents_id,
        module6_chosen_by_parents_id,
        is_bilingual,
        call0_attempt,
        call1_attempt,
        call2_attempt, 
        call3_attempt,
        call0_review,
        call1_review,
        call2_review,
        call3_review
    FROM child_supports
),

modules AS (
    SELECT 
        module_id as id,
        name,
        theme
    FROM support_modules
),

-- CTEs pour les tags utilisant les tables staging
tags AS (
    SELECT * FROM {{ ref('stg_1001mots_app__tags') }}
),

taggings AS (
    SELECT * FROM {{ ref('stg_1001mots_app__taggings') }}
),

-- CTE pour récupérer les tags par famille (taggable_type = 'Family')
tags_a AS (
    SELECT 
        t.tag_id,
        t.tag_name,
        tg.taggable_id AS family_id
    FROM taggings AS tg
    INNER JOIN tags AS t
        ON tg.tag_id = t.tag_id
    WHERE tg.taggable_type = 'ChildSupport' AND tg.tag_id IN ('876','874','901','900','893','900')
),

-- CTE pour créer une liste de tags par famille
list_of_tags AS (
    SELECT 
        taggings.taggable_id AS family_id,
        LISTAGG(tags.tag_name, ',') within group(order by tags.tag_name) as tag_list
    FROM taggings 
    INNER JOIN tags AS tags
        ON tags.tag_id = taggings.tag_id
    WHERE taggings.taggable_type = 'ChildSupport'
    group by 1
),

-- CTE permettant de récupérer le nom des groupes
groups_info AS (
    SELECT 
        group_id, 
        date_created as created_at,
        date_started as started_at,
        group_name as name,
        is_excluded_from_analytics
    FROM groups
),

supporter AS (
    SELECT 
        supporter_id as id,
        email
    FROM admin_users
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

-- Requête principale permettant de rassembler toutes les infos
SELECT distinct
    concat(cpf.family_id, '_', yc.child_id) as ind,
    cpf.family_id,
    yc.child_id,
    f.created_at AS support_creation_date,
    yc.gender,
    yc.birthdate,
    yc.ages AS age_today_in_months,
    {{ get_age_range_at_start_of_cohort('yc.birthdate', 'g.started_at') }} AS age_range_at_start_of_cohort,
    yc.group_status,
    yc.group_end as end_of_active_status,
    yc.number_of_children,
    -- Tags d'engagement basés sur les IDs spécifiques mentionnés dans les commentaires
    CASE 
        WHEN t1.tag_id IS NOT NULL THEN 1 
        ELSE 0 
    END AS is_desengage_T2,
    CASE 
        WHEN t2.tag_id IS NOT NULL THEN 1 
        ELSE 0 
    END AS is_estime_desengage_T2,
    CASE 
        WHEN t3.tag_id IS NOT NULL THEN 1 
        ELSE 0 
    END AS is_desengage_T1,
    CASE 
        WHEN t4.tag_id IS NOT NULL THEN 1 
        ELSE 0 
    END AS is_estime_desengage_T1_conserve,
    CASE 
        WHEN t5.tag_id IS NOT NULL THEN 1 
        ELSE 0 
    END AS is_estime_desengage_T1,
    CASE 
        WHEN t6.tag_id IS NOT NULL THEN 1 
        ELSE 0 
    END AS is_estime_desengage_T2_conserve,
    {{ get_engagement_state_t1('t3.tag_id', 't4.tag_id', 't5.tag_id') }} AS engagement_state_t1,
    {{ get_engagement_state_t2('t1.tag_id', 't2.tag_id', 't6.tag_id') }} AS engagement_state_t2,
    p1.city_name AS parent1_city_name,
    so.name AS source_name,
    so.channel AS source_channel,
    so.department AS source_department,
    p1.postal_code AS parent1_postal_code,
    SUBSTR(p1.postal_code, 1, 2) AS departement,
    p1.degree AS parent1_degree,
    g.name AS cohort_name,
    s.email AS supporter_name, 
    f.call0_status AS call_0_status,
    f.call1_status,
    f.call2_status,
    f.call3_status,
    f.call1_previous_goals_follow_up,
    f.call2_previous_goals_follow_up,
    f.call4_previous_goals_follow_up,
    f.is_call0_goals,
    f.is_call1_goals,
    f.is_call2_goals,
    f.is_call3_goals,
    f.is_call0_status,
    f.is_call1_status,
    f.is_call2_status,
    f.is_call3_status,
    {{ get_number_of_calls('f.is_call0_status', 'f.is_call1_status', 'f.is_call2_status', 'f.is_call3_status') }} AS number_of_calls,
    f.call0_duration AS call_0_duration,
    f.call1_duration,
    f.call2_duration,
    f.call3_duration,
    {{ get_call_review('f.call0_review') }} AS review_call0,
    {{ get_call_review('f.call1_review') }} AS review_call1,
    {{ get_call_review('f.call2_review') }} AS review_call2,
    {{ get_call_review('f.call3_review') }} AS review_call3,
    {{ get_call_attempt('f.call0_attempt') }} AS nb_of_tries_call0,
    {{ get_call_attempt('f.call1_attempt') }} AS nb_of_tries_call1,
    {{ get_call_attempt('f.call2_attempt') }} AS nb_of_tries_call2,
    {{ get_call_attempt('f.call3_attempt') }} AS nb_of_tries_call3,
    COALESCE(p1.mid_term_rate, p2.mid_term_rate) AS mid_term_rate,
    COALESCE(p1.mid_term_reaction, p2.mid_term_reaction) AS mid_term_reaction,
    m2.name AS module2_name,
    m3.name AS module3_name,
    m4.name AS module4_name,
    m5.name AS module5_name,
    m6.name AS module6_name, 
    {{ get_is_bilingual('is_bilingual') }} AS is_bilingue,
    {{ get_registration_delay('g.started_at', 'f.created_at') }} AS registration_delay,
    {{ get_age_at_registration('f.created_at', 'yc.birthdate') }} AS age_at_registration,
    lot.tag_list,
    g.is_excluded_from_analytics,
    (f.call0_status = 'OK' OR f.call1_status = 'OK') as is_call_0_1_status_OK
FROM child_parent_family AS cpf
INNER JOIN youngest_child AS yc
    ON yc.child_id = cpf.child_id
INNER JOIN parent AS p1
    ON p1.parent_id = cpf.parent1_id 
LEFT JOIN parent AS p2
    ON p2.parent_id = cpf.parent2_id
LEFT JOIN source AS so
    ON so.child_id = yc.child_id
INNER JOIN families AS f
    ON cpf.family_id = f.family_id
LEFT JOIN groups_info AS g
    ON g.group_id = yc.group_id
LEFT JOIN supporter AS s
    ON cpf.supporter_id = s.id
-- Jointures avec les tags d'engagement
LEFT JOIN tags_a AS t1
    ON t1.family_id = cpf.family_id
    AND t1.tag_id = '876' 
LEFT JOIN tags_a AS t2
    ON t2.family_id = cpf.family_id
    AND t2.tag_id = '874' 
LEFT JOIN tags_a AS t3
    ON t3.family_id = cpf.family_id
    AND t3.tag_id = '901' 
LEFT JOIN tags_a AS t4
    ON t4.family_id = cpf.family_id
    AND t4.tag_id = '900'
LEFT JOIN tags_a AS t5
    ON t5.family_id = cpf.family_id
    AND t5.tag_id = '893'
LEFT JOIN tags_a AS t6
    ON t6.family_id = cpf.family_id
    AND t6.tag_id = '877'
LEFT JOIN modules AS m2
    ON m2.id = f.module2_chosen_by_parents_id
LEFT JOIN modules AS m3
    ON m3.id = f.module3_chosen_by_parents_id
LEFT JOIN modules AS m4
    ON m4.id = f.module4_chosen_by_parents_id
LEFT JOIN modules AS m5
    ON m5.id = f.module5_chosen_by_parents_id
LEFT JOIN modules AS m6
    ON m6.id = f.module6_chosen_by_parents_id
LEFT JOIN list_of_tags AS lot
    ON lot.family_id = cpf.family_id