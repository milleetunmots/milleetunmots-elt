with admin_users as (
    select *
    from {{ ref('stg_1001mots_app__admin_users') }}
),

child_supports as (
    select *
    from {{ ref('stg_1001mots_app__child_supports') }}
),

children_sources as (
    select *
    from {{ ref('stg_1001mots_app__children_sources') }}
),

children as (
    select *
    from {{ ref('stg_1001mots_app__children') }}
),

parents as (
    select *
    from {{ ref('stg_1001mots_app__parents') }}
),

groups as (
    select *
    from {{ ref('stg_1001mots_app__groups') }}
),

sources as (
    select *
    from {{ ref('stg_1001mots_app__sources') }}
),

versions as (
    select *
    from {{ ref('stg_1001mots_app__versions') }}
),

-- Enrichissement des données enfants avec les informations des parents
child_parent_info as (
    select 
        c.id as child_id,
        c.group_id as group_id,
        c.date_created as child_created_at,
        c.date_birth as child_birth_date,
        c.group_status as child_status,
        c.parent1_id,
        c.parent2_id,
        p1.city_name as parent1_city,
        p1.postal_code as parent1_postal_code,
        p1.degree as parent1_degree,
        p1.mid_term_rate as parent1_mid_term_rate,
        p1.mid_term_reaction as parent1_mid_term_reaction,
        p2.city_name as parent2_city,
        p2.postal_code as parent2_postal_code,
        p2.degree as parent2_degree,
        p2.mid_term_rate as parent2_mid_term_rate,
        p2.mid_term_reaction as parent2_mid_term_reaction
    from children c
    left join parents p1
        on c.parent1_id = p1.parent_id
    left join parents p2
        on c.parent2_id = p2.parent_id
    where c.date_discarded is null
),

-- Enrichissement avec les informations de groupe
child_group_info as (
    select 
        cpi.*,
        g.group_name,
        g.date_started as group_start_date,
        g.date_ended as group_end_date,
        g.is_excluded_from_analytics,
        case 
            when g.date_ended is null then g.date_started + interval '12 months'
            else g.date_ended 
        end as group_expected_end_date
    from child_parent_info cpi
    left join groups g
        on cpi.group_id = g.group_id
),

-- Enrichissement avec les informations de source
child_source_info as (
    select 
        cgi.*,
        s.name as source_name,
        s.channel as source_channel,
        s.department as source_department
    from child_group_info cgi
    left join children_sources cs
        on cgi.child_id = cs.child_id
    left join sources s
        on cs.source_id = s.source_id
),

-- Enrichissement avec les informations de support
child_support_info as (
    select 
        csi.*,
        cs.family_id,
        au.admin_user_id as supporter_id,
        au.email as supporter_email
    from child_source_info csi
    left join child_supports cs
        on csi.child_id = cs.child_id
    left join admin_users au
        on cs.supporter_id = au.admin_user_id
),

-- Calcul des indicateurs temporels
child_temporal_metrics as (
    select 
        csi.*,
        -- Âge à l'inscription
        (date_part('year', child_created_at) - date_part('year', child_birth_date)) * 12 + 
        (date_part('month', child_created_at) - date_part('month', child_birth_date)) as age_at_registration_months,
        
        -- Durée d'accompagnement
        case 
            when group_end_date is not null then
                (date_part('year', group_end_date) - date_part('year', group_start_date)) * 12 + 
                (date_part('month', group_end_date) - date_part('month', group_start_date))
            else
                (date_part('year', current_date) - date_part('year', group_start_date)) * 12 + 
                (date_part('month', current_date) - date_part('month', group_start_date))
        end as support_duration_months,
        
        -- Statut actuel
        case 
            when group_end_date is not null then 'Terminé'
            when group_start_date is not null then 'En cours'
            else 'En attente'
        end as current_status
    from child_support_info csi
)

-- Table finale avec tous les indicateurs
select 
    -- Identifiants
    child_id,
    family_id,
    supporter_id,
    
    -- Informations de base
    child_created_at,
    child_birth_date,
    child_status,
    age_at_registration_months,
    
    -- Informations des parents
    parent1_city,
    parent1_postal_code,
    parent1_degree,
    parent1_mid_term_rate,
    parent1_mid_term_reaction,
    parent2_city,
    parent2_postal_code,
    parent2_degree,
    parent2_mid_term_rate,
    parent2_mid_term_reaction,
    
    -- Informations de groupe
    group_name,
    group_start_date,
    group_end_date,
    group_expected_end_date,
    support_duration_months,
    is_excluded_from_analytics,
    
    -- Informations de source
    source_name,
    source_channel,
    source_department,
    
    -- Informations de support
    supporter_email,
    
    -- Statut et indicateurs
    current_status,
    
    -- Catégorisation par âge
    case 
        when age_at_registration_months < 3 then 'A/ <3 mois'
        when age_at_registration_months <= 12 then 'B/ 3-12 mois'
        when age_at_registration_months <= 18 then 'C/ 12-18 mois'
        when age_at_registration_months <= 24 then 'D/ 18-24 mois'
        when age_at_registration_months <= 30 then 'E/ 24-30 mois'
        when age_at_registration_months > 30 then 'F/ >30 mois'
        else 'G/ NSP' 
    end as age_category

from child_temporal_metrics