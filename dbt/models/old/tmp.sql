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

versions AS (
    SELECT * FROM {{ source('mots_app', 'versions') }}
),

-- CTEs métier
child_parent_family AS (
    SELECT 
        c.created_at,
        c.id AS child_id,
        p1.id AS parent1_id, 
        p2.id AS parent2_id,
        cs.id AS family_id,
        au.id AS supporter_id,
        g.name,
        CASE 
            WHEN p1.gender = 'm' THEN 1
            WHEN p2.gender = 'm' THEN 1 
            ELSE NULL END AS is_a_father 
    FROM children AS c
    LEFT JOIN parents AS p1 ON p1.id = c.parent1_id 
    LEFT JOIN parents AS p2 ON p2.id = c.parent2_id 
    LEFT JOIN child_supports AS cs ON c.child_support_id = cs.id
    LEFT JOIN groups AS g ON c.group_id = g.id
    LEFT JOIN admin_users AS au ON au.id = cs.supporter_id
    WHERE c.discarded_at IS NULL
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

change_status AS (
    SELECT 
        item_id,
        object_changes->'updated_at'->>0 AS updated_at,
        object_changes->'group_status'->>1 AS group_status
    FROM children AS cs 
    LEFT JOIN versions AS v ON v.item_id = cs.id
    WHERE item_type = 'Child' AND object_changes->'group_status'->>1 IN ('stopped', 'disengaged')
),

child_data AS (
    SELECT 
        c.id AS child_id, 
        c.created_at,
        g.name,
        g.is_excluded_from_analytics,
        g.started_at, 
        CASE WHEN g.ended_at IS NULL THEN g.started_at + INTERVAL '12 months' ELSE g.ended_at END AS ended_at_clean,
        SUBSTR(TRIM(p.postal_code), 1, 2) AS departement,
        c.birthdate AS birthdate,
        (DATE_PART('year', c.created_at) - DATE_PART('year', c.birthdate)) * 12 + (DATE_PART('month', c.created_at) - DATE_PART('month', c.birthdate)) AS age_at_registration,
        c.group_status AS child_status
    FROM children AS c
    LEFT JOIN parents AS p ON c.parent1_id = p.id
    LEFT JOIN groups AS g ON c.group_id = g.id
),

child_lead AS (
    SELECT 
        cd.child_id,
        cd.name, 
        DATE(cd.created_at) AS created_at,
        DATE(cd.started_at) AS started_at, 
        DATE(cd.ended_at_clean) AS ended_at_clean,
        DATE(cs.updated_at) AS updated_at,
        cs.group_status,
        CASE 
            WHEN DATE(ended_at_clean) <= DATE(updated_at) THEN DATE(ended_at_clean)
            WHEN DATE(updated_at) <= DATE(ended_at_clean) THEN DATE(updated_at)
            ELSE DATE(ended_at_clean) END AS ended_at_perso,
        cd.departement,
        cd.age_at_registration,
        cd.child_status
    FROM child_data AS cd 
    LEFT JOIN change_status AS cs ON cd.child_id = cs.item_id
    WHERE is_excluded_from_analytics = false OR is_excluded_from_analytics IS NULL
)

SELECT 
    cpf.family_id,
    cl.name as cohort_name,
    --- FILTERS --- 
    s.channel AS source_channel,
    s.name AS source_name,
    p.postal_code AS family_postal_code,
    p.city_name AS family_city,
    DATE_TRUNC('year', cpf.created_at) AS year_of_registration,
    SUBSTR(p.postal_code, 1, 2) AS departement,
    --- END FILTERS ---
    cpf.child_id,
    cpf.is_a_father,
    DATE_TRUNC('month', cpf.created_at) AS month_of_registration,
    cpf.created_at AS date_of_registration,
    CASE 
        WHEN cl.age_at_registration < 3 THEN 'A/ <3 mois'
        WHEN cl.age_at_registration <= 12 THEN 'B/ 3-12 mois'
        WHEN cl.age_at_registration <= 18 THEN 'C/ 12-18 mois'
        WHEN cl.age_at_registration <= 24 THEN 'D/ 18-24 mois'
        WHEN cl.age_at_registration <= 30 THEN 'E/ 24-30 mois'
        WHEN cl.age_at_registration > 30 THEN 'F/ >30 mois'
        ELSE 'G/ NSP' END AS child_age_in_month, 
    CASE 
        WHEN ended_at_perso >= date_trunc('year', now() - interval '2 year') AND started_at <= date_trunc('year', now() - interval '2 year') THEN 1 -- Nombre d'enfants accompagnes au premier janvier
        WHEN extract(year from started_at) = extract(year from current_date - interval '2 year') THEN 1 -- Nombre d'enfants ayant commencé un accompagnement à l'année N
        WHEN extract(year from cl.created_at) = extract(year from current_date - interval '2 year') AND (extract(year from started_at) = extract(year from current_date - interval '1 year')) THEN 1 -- Nombre d'enfants inscrits à l'année N dont l'accompagnement n'a pas encore commencé
        WHEN extract(year from cl.created_at) = extract(year from current_date - interval '2 year') /*AND started_at IS NULL*/ AND child_status = 'waiting' THEN 1 -- Nombre d'enfants inscrits à l'année N dont l'accompagnement n'a pas encore commencé et n'est pas encore planifié 
        ELSE 0 END AS accompagnement_annee_n_moins_2, 
    CASE 
        WHEN ended_at_perso >= date_trunc('year', now() - interval '1 year') AND started_at <= date_trunc('year', now() - interval '1 year') THEN 1 -- Nombre d'enfants accompagnes au premier janvier
        WHEN extract(year from started_at) = extract(year from current_date - interval '1 year') THEN 1 -- Nombre d'enfants ayant commencé un accompagnement à l'année N
        WHEN extract(year from cl.created_at) = extract(year from current_date - interval '1 year') AND (extract(year from started_at) = extract(year from current_date)) THEN 1 -- Nombre d'enfants inscrits à l'année N dont l'accompagnement n'a pas encore commencé
        WHEN extract(year from cl.created_at) = extract(year from current_date - interval '1 year') /*AND started_at IS NULL*/ AND child_status = 'waiting' THEN 1  -- Nombre d'enfants inscrits à l'année N dont l'accompagnement n'a pas encore commencé et n'est pas encore planifié
        ELSE 0 END AS accompagnement_annee_n_moins_1,
    CASE 
        WHEN ended_at_perso >= date_trunc('year', now() - interval '1 year') AND started_at <= date_trunc('year', now() - interval '1 year') THEN 'Enfant accompagne 1 janvier' 
        WHEN extract(year from started_at) = extract(year from current_date - interval '1 year') THEN 'Enfant ayant commence son accompagnement dans annee'
        WHEN extract(year from cl.created_at) = extract(year from current_date - interval '1 year') AND (extract(year from started_at) = extract(year from current_date)) THEN 'Enfant inscrits dont accompagnement pas commence'
        WHEN extract(year from cl.created_at) = extract(year from current_date - interval '1 year') /*AND started_at IS NULL*/ AND child_status = 'waiting' THEN 'Enfant inscrits dont accompagnement pas commence et pas planifié'
        ELSE null END AS accompagnement_annee_n_moins_1_decompose,
    CASE 
        WHEN ended_at_perso >= date_trunc('year', now()) AND started_at <= date_trunc('year', now()) THEN 1 -- Nombre d'enfants accompagnes au premier janvier
        WHEN extract(year from started_at) = extract(year from current_date) THEN 1 -- Nombre d'enfants ayant commencé un accompagnement à l'année N
        WHEN extract(year from cl.created_at) = extract(year from current_date) AND (extract(year from started_at) = extract(year from current_date + interval '1 year')) THEN 1 -- Nombre d'enfants inscrits à l'année N dont l'accompagnement n'a pas encore commencé
        WHEN extract(year from cl.created_at) = extract(year from current_date) /*AND started_at IS NULL*/ AND child_status = 'waiting' THEN 1 -- Nombre d'enfants inscrits à l'année N dont l'accompagnement n'a pas encore commencé et n'est pas encore planifié
        ELSE 0 END AS accompagnement_annee_n,
    CASE 
        WHEN ended_at_perso >= date_trunc('year', now()) AND started_at <= date_trunc('year', now()) THEN 'Enfant accompagne 1 janvier' -- Nombre d'enfants accompagnes au premier janvier
        WHEN extract(year from started_at) = extract(year from current_date) THEN 'Enfant ayant commence son accompagnement dans annee' -- Nombre d'enfants ayant commencé un accompagnement à l'année N
        WHEN extract(year from cl.created_at) = extract(year from current_date) AND (extract(year from started_at) = extract(year from current_date + interval '1 year')) THEN 'Enfant inscrits dont accompagnement pas commence' -- Nombre d'enfants inscrits à l'année N dont l'accompagnement n'a pas encore commencé
        WHEN extract(year from cl.created_at) = extract(year from current_date) /*AND started_at IS NULL*/ AND child_status = 'waiting' THEN 'Enfant inscrits dont accompagnement pas commence et pas planifié' -- Nombre d'enfants inscrits à l'année N dont l'accompagnement n'a pas encore commencé et n'est pas encore planifié
        ELSE null END AS accompagnement_annee_n_decompose,      
    CASE 
        WHEN ended_at_perso >= date_trunc('year', now() + interval '1 year') AND started_at <= date_trunc('year', now() + interval '1 year') THEN 1 -- Nombre d'enfants accompagnes au premier janvier
        WHEN extract(year from started_at) = extract(year from current_date + interval '1 year') THEN 1 -- Nombre d'enfants ayant commencé un accompagnement à l'année N
        WHEN extract(year from cl.created_at) = extract(year from current_date + interval '1 year') THEN 1 -- Nombre d'enfants inscrits à l'année N dont l'accompagnement n'a pas encore commencé
        ELSE 0 END AS accompagnement_annee_n_1,
    CASE 
        -- cohorte a moins de 3 mois à la fin de l'année en cours
        WHEN (DATE_PART('year', (date_trunc('year', now() + interval '1 year') - interval '1 day')) - DATE_PART('year',  started_at)) * 12 + (DATE_PART('month', (date_trunc('year', now() + interval '1 year') - interval '1 day')) - DATE_PART('month', started_at)) < 3
        THEN 1
        -- cohorte a moins de 6 mois aujourd'hui, aura plus de 6 mois et moins de 12 mois à la fin de l'année en cours
        WHEN (DATE_PART('year', (date_trunc('year', now() + interval '1 year') - interval '1 day')) - DATE_PART('year',  started_at)) * 12 + (DATE_PART('month', (date_trunc('year', now() + interval '1 year') - interval '1 day')) - DATE_PART('month', started_at)) BETWEEN 6 AND 11
        AND  (DATE_PART('year', CURRENT_DATE) - DATE_PART('year',  started_at)) * 12 + (DATE_PART('month',CURRENT_DATE) - DATE_PART('month', started_at)) < 6 
        THEN 0.8
        -- cohorte a moins de 6 mois aujourd'hui, aura plus de 3 mois et moins de 12 mois à la fin de l'année en cours 
        WHEN (DATE_PART('year', (date_trunc('year', now() + interval '1 year') - interval '1 day')) - DATE_PART('year',  started_at)) * 12 + (DATE_PART('month', (date_trunc('year', now() + interval '1 year') - interval '1 day')) - DATE_PART('month', started_at)) BETWEEN 3 AND 11
        AND  (DATE_PART('year', CURRENT_DATE) - DATE_PART('year',  started_at)) * 12 + (DATE_PART('month',CURRENT_DATE) - DATE_PART('month', started_at)) < 6 
        THEN 0.6
        -- cohorte a plus de 6 mois aujourd'hui et la date de fin perso est supérieure à celle de la fin de l'année en cours
        WHEN (DATE_PART('year', CURRENT_DATE) - DATE_PART('year',  started_at)) * 12 + (DATE_PART('month',CURRENT_DATE) - DATE_PART('month', started_at)) >= 6
        AND (DATE_PART('year', (date_trunc('year', now() + interval '1 year') - interval '1 day')) - DATE_PART('year',  ended_at_perso)) * 12 + (DATE_PART('month', (date_trunc('year', now() + interval '1 year') - interval '1 day')) - DATE_PART('month', ended_at_perso)) <= 0
        THEN 1 
        ELSE 0 END AS accompagnement_annee_n_1_ajuste,
    CASE 
        -- cohorte a moins de 3 mois à la fin de l'année en cours
        WHEN (DATE_PART('year', (date_trunc('year', now() + interval '1 year') - interval '1 day')) - DATE_PART('year',  started_at)) * 12 + (DATE_PART('month', (date_trunc('year', now() + interval '1 year') - interval '1 day')) - DATE_PART('month', started_at)) < 3
        THEN 1
        -- cohorte a moins de 6 mois aujourd'hui, aura plus de 6 mois et moins de 12 mois à la fin de l'année en cours
        WHEN (DATE_PART('year', (date_trunc('year', now() + interval '1 year') - interval '1 day')) - DATE_PART('year',  started_at)) * 12 + (DATE_PART('month', (date_trunc('year', now() + interval '1 year') - interval '1 day')) - DATE_PART('month', started_at)) BETWEEN 6 AND 11
        AND  (DATE_PART('year', CURRENT_DATE) - DATE_PART('year',  started_at)) * 12 + (DATE_PART('month',CURRENT_DATE) - DATE_PART('month', started_at)) < 6 
        THEN 0.8
        -- cohorte a moins de 6 mois aujourd'hui, aura plus de 3 mois et moins de 12 mois à la fin de l'année en cours 
        WHEN (DATE_PART('year', (date_trunc('year', now() + interval '1 year') - interval '1 day')) - DATE_PART('year',  started_at)) * 12 + (DATE_PART('month', (date_trunc('year', now() + interval '1 year') - interval '1 day')) - DATE_PART('month', started_at)) BETWEEN 3 AND 11
        AND  (DATE_PART('year', CURRENT_DATE) - DATE_PART('year',  started_at)) * 12 + (DATE_PART('month',CURRENT_DATE) - DATE_PART('month', started_at)) < 6 
        THEN 0.6
        -- cohorte a plus de 6 mois aujourd'hui et la date de fin perso est supérieure à celle de la fin de l'année en cours
        WHEN (DATE_PART('year', CURRENT_DATE) - DATE_PART('year',  started_at)) * 12 + (DATE_PART('month',CURRENT_DATE) - DATE_PART('month', started_at)) >= 6
        AND (DATE_PART('year', (date_trunc('year', now() + interval '1 year') - interval '1 day')) - DATE_PART('year',  ended_at_perso)) * 12 + (DATE_PART('month', (date_trunc('year', now() + interval '1 year') - interval '1 day')) - DATE_PART('month', ended_at_perso)) <= 0
        THEN 1 
        ELSE 0 END AS accompagnement_annee_n_1_ajuste_decompose

FROM child_lead AS cl 
LEFT JOIN source AS s ON cl.child_id = s.child_id 
LEFT JOIN child_parent_family AS cpf ON cpf.child_id = cl.child_id
LEFT JOIN parent AS p ON p.parent_id = cpf.parent1_id
