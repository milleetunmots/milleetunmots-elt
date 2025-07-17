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

support_modules AS (
    SELECT * FROM {{ source('mots_app', 'support_modules') }}
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
        au.id AS supporter_id
    FROM children AS c
    LEFT JOIN parents AS p1 ON p1.id = c.parent1_id 
    LEFT JOIN parents AS p2 ON p2.id = c.parent2_id 
    LEFT JOIN child_supports AS cs ON c.child_support_id = cs.id
    LEFT JOIN admin_users AS au ON au.id = cs.supporter_id
    WHERE c.discarded_at IS NULL
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
            COUNT( distinct id) AS number_of_children
        FROM children
        GROUP BY child_support_id
    ) AS max_birthdate
    ON max_birthdate.child_support_id = ch.child_support_id AND max_birthdate.youngest_child_birthdate = ch.birthdate
),

parent AS (
    SELECT 
        id AS parent_id,
        city_name, 
        trim(postal_code) AS postal_code,
        degree, 
        mid_term_rate,
        mid_term_reaction
    FROM parents 
),

families AS (
    SELECT 
        id AS family_id,
        created_at,
        call0_status,
        call1_status,
        call2_status,
        call3_status,
        CASE 
            WHEN call1_previous_goals_follow_up IS NULL OR call1_previous_goals_follow_up = '' THEN NULL
            WHEN call1_previous_goals_follow_up = '1_succeed' THEN 'PM réussie'
            WHEN call1_previous_goals_follow_up = '2_tried' THEN 'PM essayée'
            WHEN call1_previous_goals_follow_up = '3_no_tried' THEN 'PM non essayée'
            WHEN call1_previous_goals_follow_up = '4_no_goal' THEN 'Pas de PM' ELSE call1_previous_goals_follow_up END AS call1_previous_goals_follow_up,
        CASE 
            WHEN call2_previous_goals_follow_up IS NULL OR call2_previous_goals_follow_up = '' THEN NULL
            WHEN call2_previous_goals_follow_up = '1_succeed' THEN 'PM réussie'
            WHEN call2_previous_goals_follow_up = '2_tried' THEN 'PM essayée'
            WHEN call2_previous_goals_follow_up = '3_no_tried' THEN 'PM non essayée'
            WHEN call2_previous_goals_follow_up = '4_no_goal' THEN 'Pas de PM' ELSE call2_previous_goals_follow_up END AS call2_previous_goals_follow_up,
        CASE 
            WHEN call4_previous_goals_follow_up IS NULL OR call4_previous_goals_follow_up = '' THEN NULL
            WHEN call4_previous_goals_follow_up = '1_succeed' THEN 'PM réussie'
            WHEN call4_previous_goals_follow_up = '2_tried' THEN 'PM essayée'
            WHEN call4_previous_goals_follow_up = '3_no_tried' THEN 'PM non essayée'
            WHEN call4_previous_goals_follow_up = '4_no_goal' THEN 'Pas de PM' ELSE call4_previous_goals_follow_up END AS call4_previous_goals_follow_up,
        CASE WHEN call0_goals_sms IS NULL THEN 0 WHEN call0_goals_sms = '' THEN 0 ELSE 1 END AS is_call0_goals,
        CASE WHEN call1_goals_sms IS NULL THEN 0 WHEN call1_goals_sms = '' THEN 0 ELSE 1 END AS is_call1_goals,
        CASE WHEN call2_goals_sms IS NULL THEN 0 WHEN call2_goals_sms = '' THEN 0 ELSE 1 END AS is_call2_goals,
        CASE WHEN call3_goals_sms IS NULL THEN 0 WHEN call3_goals_sms = '' THEN 0 ELSE 1 END AS is_call3_goals,
        CASE WHEN call0_status IS NULL THEN 0 WHEN call0_status = '' THEN 0 ELSE 1 END AS is_call0_status,
        CASE WHEN call1_status IS NULL THEN 0 WHEN call1_status = '' THEN 0 ELSE 1 END AS is_call1_status,
        CASE WHEN call2_status IS NULL THEN 0 WHEN call2_status = '' THEN 0 ELSE 1 END AS is_call2_status,
        CASE WHEN call3_status IS NULL THEN 0 WHEN call3_status = '' THEN 0 ELSE 1 END AS is_call3_status,
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
        id,
        name,
        theme
    FROM support_modules
),

tags_a AS (
    SELECT 
        t.id AS tag_id,
        t.name AS tag_name,
        tg.taggable_id AS family_id
    FROM taggings AS tg
    INNER JOIN tags AS t ON tg.tag_id = t.id
    WHERE tg.taggable_type = 'ChildSupport' AND tg.tag_id IN (876,874,901,900,893,900)
),

list_of_tags AS (
    SELECT 
        taggings.taggable_id AS family_id,
        LISTAGG(tags.name, ',') within group(order by tags.name) as tag_list
    FROM taggings 
    INNER JOIN tags AS tags ON tags.id = taggings.tag_id
    WHERE taggings.taggable_type = 'ChildSupport'
    GROUP BY 1 
),

supporter AS (
    SELECT 
        id,
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
    INNER JOIN sources AS s ON cs.source_id = s.id
)

-- Requête principale permettant de rassembler toutes les infos
SELECT DISTINCT
        concat(cpf.family_id, '_', yc.child_id) as ind,
        cpf.family_id,
        yc.child_id,
        -- To remove to_date
        to_date(f.created_at) AS support_creation_date,
        yc.gender,
        -- To remove to_date
        to_date(yc.birthdate) AS birthdate,
        yc.ages AS age_today_in_months,
        CASE 
            WHEN (DATE_PART('year', g.started_at) - DATE_PART('year', yc.birthdate)) * 12 + (DATE_PART('month', g.started_at) - DATE_PART('month', yc.birthdate)) < 12 THEN '0-11'
            WHEN (DATE_PART('year', g.started_at) - DATE_PART('year', yc.birthdate)) * 12 + (DATE_PART('month', g.started_at) - DATE_PART('month', yc.birthdate)) < 24 THEN '12-23'
            WHEN (DATE_PART('year', g.started_at) - DATE_PART('year', yc.birthdate)) * 12 + (DATE_PART('month', g.started_at) - DATE_PART('month', yc.birthdate)) < 37 THEN '24-36'
            ELSE NULL
        END AS age_range_at_start_of_cohort,
        yc.group_status,
        -- To remove to_date
        to_date(yc.group_end) AS end_of_active_status,
        yc.number_of_children,
        CASE WHEN t1.tag_name IS NOT NULL THEN 1 ELSE 0 END AS is_desengage_T2,
        CASE WHEN t2.tag_name IS NOT NULL THEN 1 ELSE 0 END AS is_estime_desengage_T2,
        CASE WHEN t3.tag_name IS NOT NULL THEN 1 ELSE 0 END AS is_desengage_T1,
        CASE WHEN t4.tag_name IS NOT NULL THEN 1 ELSE 0 END AS is_estime_desengage_T1_conserve,
        CASE WHEN t5.tag_name IS NOT NULL THEN 1 ELSE 0 END AS is_estime_desengage_T1,
        CASE WHEN t6.tag_name IS NOT NULL THEN 1 ELSE 0 END AS is_estime_desengage_T2_conserve,
        CASE 
            WHEN t4.tag_name IS NOT NULL THEN 'Estimé désengagé t1 conservé' 
            WHEN t3.tag_name IS NOT NULL THEN 'Désengagé t1'
            ELSE 'Conservé t1'
        END AS engagement_state_t1,
        CASE
            WHEN t6.tag_name IS NOT NULL THEN 'Estimé désengagé t2 conservé'
            WHEN t1.tag_name IS NOT NULL THEN 'Désengagé t2'
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
            WHEN f.call0_review = '0_very_satisfied' THEN 'très satisfaisant'
            WHEN f.call0_review = '1_rather_satisfied' THEN 'satisfaisant'
            WHEN f.call0_review = '2_rather_dissatisfied' THEN 'peu satisfaisant'
            WHEN f.call0_review = '3_very_dissatisfied' THEN 'très insatisfaisant'
            WHEN f.call0_review = '' THEN 'vide'
            ELSE f.call0_review
        END AS review_call0,
        CASE 
            WHEN f.call1_review = '0_very_satisfied' THEN 'très satisfaisant'
            WHEN f.call1_review = '1_rather_satisfied' THEN 'satisfaisant'
            WHEN f.call1_review = '2_rather_dissatisfied' THEN 'peu satisfaisant'
            WHEN f.call1_review = '3_very_dissatisfied' THEN 'très insatisfaisant'
            WHEN f.call0_review = '' THEN 'vide'
            ELSE f.call1_review
        END AS review_call1,
        CASE 
            WHEN f.call2_review = '0_very_satisfied' THEN 'très satisfaisant'
            WHEN f.call2_review = '1_rather_satisfied' THEN 'satisfaisant'
            WHEN f.call2_review = '2_rather_dissatisfied' THEN 'peu satisfaisant'
            WHEN f.call2_review = '3_very_dissatisfied' THEN 'très insatisfaisant'
            WHEN f.call0_review = '' THEN 'vide'
            ELSE f.call2_review
        END AS review_call2,
        CASE 
            WHEN f.call3_review = '0_very_satisfied' THEN 'très satisfaisant'
            WHEN f.call3_review = '1_rather_satisfied' THEN 'satisfaisant'
            WHEN f.call3_review = '2_rather_dissatisfied' THEN 'peu satisfaisant'
            WHEN f.call3_review = '3_very_dissatisfied' THEN 'très insatisfaisant'
            WHEN f.call0_review = '' THEN 'vide'
            ELSE f.call3_review
        END AS review_call3,
        CASE 
            WHEN f.call0_attempt = 'first_call_attempt' THEN '1ère tentative'
            WHEN f.call0_attempt = 'second_call_attempt' THEN '2ème tentative'
            WHEN f.call0_attempt = 'third_or_more_calls_attempt' THEN '3ème tentative'
            ELSE f.call0_attempt
        END AS nb_of_tries_call0,
        CASE 
            WHEN f.call1_attempt = 'first_call_attempt' THEN '1ère tentative'
            WHEN f.call1_attempt = 'second_call_attempt' THEN '2ème tentative'
            WHEN f.call1_attempt = 'third_or_more_calls_attempt' THEN '3ème tentative'
            ELSE f.call1_attempt
        END AS nb_of_tries_call1,
        CASE 
            WHEN f.call2_attempt = 'first_call_attempt' THEN '1ère tentative'
            WHEN f.call2_attempt = 'second_call_attempt' THEN '2ème tentative'
            WHEN f.call2_attempt = 'third_or_more_calls_attempt' THEN '3ème tentative'
            ELSE f.call2_attempt
        END AS nb_of_tries_call2,
        CASE 
            WHEN f.call3_attempt = 'first_call_attempt' THEN '1ère tentative'
            WHEN f.call3_attempt = 'second_call_attempt' THEN '2ème tentative'
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
            WHEN is_bilingual = '1_no' THEN 'Non'
            ELSE null
        END AS is_bilingue,
        (DATE_PART('year', g.started_at) - DATE_PART('year', f.created_at)) * 12 + (DATE_PART('month', g.started_at) - DATE_PART('month', f.created_at)) AS registration_delay,
        (DATE_PART('year', f.created_at) - DATE_PART('year', birthdate)) * 12 + (DATE_PART('month', f.created_at) - DATE_PART('month', birthdate)) AS age_at_registration,
        lot.tag_list,
        g.is_excluded_from_analytics,
        (f.call0_status = 'OK' OR f.call1_status = 'OK') AS is_call_0_1_status_OK
FROM child_parent_family AS cpf
INNER JOIN youngest_child AS yc ON yc.child_id = cpf.child_id
INNER JOIN parent AS p1 ON p1.parent_id = cpf.parent1_id 
LEFT JOIN parent AS p2 ON p2.parent_id = cpf.parent2_id
LEFT JOIN source AS so ON so.child_id = yc.child_id
INNER JOIN families AS f ON cpf.family_id = f.family_id
LEFT JOIN groups AS g ON g.id = yc.group_id
LEFT JOIN supporter AS s ON cpf.supporter_id = s.id
LEFT JOIN tags_a AS t1 ON t1.family_id = cpf.family_id AND t1.tag_id = 876 
LEFT JOIN tags_a AS t2 ON t2.family_id = cpf.family_id AND t2.tag_id = 874 
LEFT JOIN tags_a AS t3 ON t3.family_id = cpf.family_id AND t3.tag_id = 901 
LEFT JOIN tags_a AS t4 ON t4.family_id = cpf.family_id AND t4.tag_id = 900
LEFT JOIN tags_a AS t5 ON t5.family_id = cpf.family_id AND t5.tag_id = 893
LEFT JOIN tags_a AS t6 ON t6.family_id = cpf.family_id AND t6.tag_id = 877
LEFT JOIN modules AS m2 ON m2.id = f.module2_chosen_by_parents_id
LEFT JOIN modules AS m3 ON m3.id = f.module3_chosen_by_parents_id
LEFT JOIN modules AS m4 ON m4.id = f.module4_chosen_by_parents_id
LEFT JOIN modules AS m5 ON m5.id = f.module5_chosen_by_parents_id
LEFT JOIN modules AS m6 ON m6.id = f.module6_chosen_by_parents_id
LEFT JOIN list_of_tags AS lot ON lot.family_id = cpf.family_id