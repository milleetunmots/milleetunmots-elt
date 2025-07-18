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
        (DATE_PART('year', CURRENT_DATE) - DATE_PART('year', ch.date_birth)) * 12 + (DATE_PART('month', CURRENT_DATE) - DATE_PART('month', ch.date_birth)) AS ages,
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
        CASE 
            WHEN call1_previous_goals_follow_up IS NULL OR call1_previous_goals_follow_up = '' THEN NULL
            WHEN call1_previous_goals_follow_up = '1_succeed' THEN 'PM réussie'
            WHEN call1_previous_goals_follow_up = '2_tried' THEN 'PM essayée'
            WHEN call1_previous_goals_follow_up = '3_no_tried' THEN 'PM non essayée'
            WHEN call1_previous_goals_follow_up = '4_no_goal' THEN 'Pas de PM' 
            ELSE call1_previous_goals_follow_up 
        END AS call1_previous_goals_follow_up,
        CASE 
            WHEN call2_previous_goals_follow_up IS NULL OR call2_previous_goals_follow_up = '' THEN NULL
            WHEN call2_previous_goals_follow_up = '1_succeed' THEN 'PM réussie'
            WHEN call2_previous_goals_follow_up = '2_tried' THEN 'PM essayée'
            WHEN call2_previous_goals_follow_up = '3_no_tried' THEN 'PM non essayée'
            WHEN call2_previous_goals_follow_up = '4_no_goal' THEN 'Pas de PM' 
            ELSE call2_previous_goals_follow_up 
        END AS call2_previous_goals_follow_up,
        CASE 
            WHEN call4_previous_goals_follow_up IS NULL OR call4_previous_goals_follow_up = '' THEN NULL
            WHEN call4_previous_goals_follow_up = '1_succeed' THEN 'PM réussie'
            WHEN call4_previous_goals_follow_up = '2_tried' THEN 'PM essayée'
            WHEN call4_previous_goals_follow_up = '3_no_tried' THEN 'PM non essayée'
            WHEN call4_previous_goals_follow_up = '4_no_goal' THEN 'Pas de PM' 
            ELSE call4_previous_goals_follow_up 
        END AS call4_previous_goals_follow_up,
        CASE WHEN call0_goals_sms IS NULL OR call0_goals_sms = '' THEN 0 ELSE 1 END AS is_call0_goals,
        CASE WHEN call1_goals_sms IS NULL OR call1_goals_sms = '' THEN 0 ELSE 1 END AS is_call1_goals,
        CASE WHEN call2_goals_sms IS NULL OR call2_goals_sms = '' THEN 0 ELSE 1 END AS is_call2_goals,
        CASE WHEN call3_goals_sms IS NULL OR call3_goals_sms = '' THEN 0 ELSE 1 END AS is_call3_goals,
        CASE WHEN call0_status IS NULL OR call0_status = '' THEN 0 ELSE 1 END AS is_call0_status,
        CASE WHEN call1_status IS NULL OR call1_status = '' THEN 0 ELSE 1 END AS is_call1_status,
        CASE WHEN call2_status IS NULL OR call2_status = '' THEN 0 ELSE 1 END AS is_call2_status,
        CASE WHEN call3_status IS NULL OR call3_status = '' THEN 0 ELSE 1 END AS is_call3_status,
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
    CASE 
        WHEN (DATE_PART('year', g.started_at) - DATE_PART('year', yc.birthdate)) * 12
            + (DATE_PART('month', g.started_at) - DATE_PART('month', yc.birthdate)) < 12
            THEN '0-11'
        WHEN (DATE_PART('year', g.started_at) - DATE_PART('year', yc.birthdate)) * 12
            + (DATE_PART('month', g.started_at) - DATE_PART('month', yc.birthdate)) < 24
            THEN '12-23'
        WHEN (DATE_PART('year', g.started_at) - DATE_PART('year', yc.birthdate)) * 12
            + (DATE_PART('month', g.started_at) - DATE_PART('month', yc.birthdate)) < 37
            THEN '24-36'
        ELSE NULL 
    END AS age_range_at_start_of_cohort,
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
    CASE 
        WHEN t3.tag_id IS NOT NULL THEN 'Désengagé t1'
        WHEN t4.tag_id IS NOT NULL THEN 'Estime désengagé t1 conservé'
        --WHEN t5.tag_id IS NOT NULL THEN 'Estime désengagé t1'
        ELSE 'Conservé t1'
    END AS engagement_state_t1,
    CASE 
        WHEN t1.tag_id IS NOT NULL THEN 'Désengagé t2'
        --WHEN t2.tag_id IS NOT NULL THEN 'Estime désengagé t2'
        WHEN t6.tag_id IS NOT NULL THEN 'Estime désengagé t2 conservé'
        ELSE 'Conservé t2'
    END AS engagement_state_t2,
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
    f.is_call0_status + f.is_call1_status + f.is_call2_status + f.is_call3_status AS number_of_calls,
    f.call0_duration AS call_0_duration,
    f.call1_duration,
    f.call2_duration,
    f.call3_duration,
    CASE 
        WHEN f.call0_review = '0_very_satisfied'      THEN 'très satisfaisant' 
        WHEN f.call0_review = '1_rather_satisfied'   THEN 'satisfaisant' 
        WHEN f.call0_review = '2_rather_dissatisfied' THEN 'peu satisfaisant'  
        WHEN f.call0_review = '3_very_dissatisfied'  THEN 'très insatisfaisant' 
        WHEN f.call0_review = ''                     THEN 'vide' 
        ELSE f.call0_review 
    END AS review_call0,
    CASE 
        WHEN f.call1_review = '0_very_satisfied'      THEN 'très satisfaisant' 
        WHEN f.call1_review = '1_rather_satisfied'   THEN 'satisfaisant' 
        WHEN f.call1_review = '2_rather_dissatisfied' THEN 'peu satisfaisant'  
        WHEN f.call1_review = '3_very_dissatisfied'  THEN 'très insatisfaisant' 
        -- TYPE ERROR
        WHEN f.call0_review = ''                     THEN 'vide' 
        ELSE f.call1_review 
    END AS review_call1,
    CASE 
        WHEN f.call2_review = '0_very_satisfied'      THEN 'très satisfaisant' 
        WHEN f.call2_review = '1_rather_satisfied'   THEN 'satisfaisant' 
        WHEN f.call2_review = '2_rather_dissatisfied' THEN 'peu satisfaisant'  
        WHEN f.call2_review = '3_very_dissatisfied'  THEN 'très insatisfaisant' 
        -- TYPE ERROR
        WHEN f.call0_review = ''                     THEN 'vide' 
        ELSE f.call2_review 
    END AS review_call2,
    CASE 
        WHEN f.call3_review = '0_very_satisfied'      THEN 'très satisfaisant' 
        WHEN f.call3_review = '1_rather_satisfied'   THEN 'satisfaisant' 
        WHEN f.call3_review = '2_rather_dissatisfied' THEN 'peu satisfaisant'  
        WHEN f.call3_review = '3_very_dissatisfied'  THEN 'très insatisfaisant' 
        -- TYPE ERROR
        WHEN f.call0_review = ''                     THEN 'vide' 
        ELSE f.call3_review 
    END AS review_call3,
    CASE 
        WHEN f.call0_attempt = 'first_call_attempt'         THEN '1ère tentative'
        WHEN f.call0_attempt = 'second_call_attempt'        THEN '2ème tentative'
        WHEN f.call0_attempt = 'third_or_more_calls_attempt' THEN '3ème tentative'
        ELSE f.call0_attempt 
    END AS nb_of_tries_call0,
    CASE 
        WHEN f.call1_attempt = 'first_call_attempt'         THEN '1ère tentative'
        WHEN f.call1_attempt = 'second_call_attempt'        THEN '2ème tentative'
        WHEN f.call1_attempt = 'third_or_more_calls_attempt' THEN '3ème tentative'
        ELSE f.call1_attempt 
    END AS nb_of_tries_call1,
    CASE 
        WHEN f.call2_attempt = 'first_call_attempt'         THEN '1ère tentative'
        WHEN f.call2_attempt = 'second_call_attempt'        THEN '2ème tentative'
        WHEN f.call2_attempt = 'third_or_more_calls_attempt' THEN '3ème tentative'
        ELSE f.call2_attempt 
    END AS nb_of_tries_call2,
    CASE 
        WHEN f.call3_attempt = 'first_call_attempt'         THEN '1ère tentative'
        WHEN f.call3_attempt = 'second_call_attempt'        THEN '2ème tentative'
        WHEN f.call3_attempt = 'third_or_more_calls_attempt' THEN '3ème tentative'
        ELSE f.call3_attempt 
    END AS nb_of_tries_call3,
    COALESCE(p1.mid_term_rate, p2.mid_term_rate) AS mid_term_rate,
    COALESCE(p1.mid_term_reaction, p2.mid_term_reaction) AS mid_term_reaction,
    m2.name AS module2_name,
    m3.name AS module3_name,
    m4.name AS module4_name,
    m5.name AS module5_name,
    m6.name AS module6_name, 
    CASE 
        WHEN is_bilingual = '0_yes' THEN 'Oui'
        WHEN is_bilingual = '1_no'  THEN 'Non'
        ELSE null
    END AS is_bilingue,
    (DATE_PART('year', g.started_at) - DATE_PART('year', f.created_at)) * 12
        + (DATE_PART('month', g.started_at) - DATE_PART('month', f.created_at)) AS registration_delay,
    (DATE_PART('year', f.created_at) - DATE_PART('year', yc.birthdate)) * 12
        + (DATE_PART('month', f.created_at) - DATE_PART('month', yc.birthdate)) AS age_at_registration,
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