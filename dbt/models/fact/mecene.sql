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

child_parent_family as (
    select 
        c.date_created,
        c.id as child_id,
        p1.parent_id as parent1_id, 
        p2.parent_id as parent2_id,
        cs.family_id,
        au.admin_user_id as supporter_id,
        g.group_name,
        case 
            when p1.gender = 'm' then 1
            when p2.gender = 'm' then 1 
            else null 
        end as is_a_father 
    from children c
    left join parents p1
        on p1.parent_id = c.parent1_id 
    left join parents p2
        on p2.parent_id = c.parent2_id 
    left join child_supports cs 
        on c.child_support_id = cs.family_id
    left join groups g
        on c.group_id = g.group_id
    left join admin_users au
        on au.admin_user_id = cs.supporter_id
    where c.date_discarded is null
),

source as (
    select 
        css.child_id,
        css.source_id,
        s.name, 
        s.channel, 
        s.department
    from children_sources css
    inner join sources s
        on css.source_id = s.source_id
),

parent as (
    select 
        parent_id,
        city_name, 
        trim(postal_code) as postal_code,
        degree, 
        mid_term_rate,
        mid_term_reaction
    from parents
),

change_status as (
    select 
        item_id,
        parse_json(object_changes):"date_updated"[0] as date_updated,
        parse_json(object_changes):"group_status"[1] as group_status
    from children c 
    left join versions v
        on v.item_id = c.id
    where item_type = 'Child' 
        and parse_json(object_changes):"group_status"[1] in ('stopped', 'disengaged')
),

child_data as (
    select 
        c.id as child_id, 
        c.date_created,
        g.group_name,
        g.is_excluded_from_analytics,
        g.date_started, 
        case 
            when g.date_ended is null then g.date_started + interval '12 months' 
            else g.date_ended 
        end as ended_at_clean,
        substr(trim(p.postal_code), 1, 2) as departement,
        c.date_birth as birthdate,
        (date_part('year', c.date_created) - date_part('year', c.date_birth)) * 12 + 
        (date_part('month', c.date_created) - date_part('month', c.date_birth)) as age_at_registration,
        c.group_status as child_status
    from children c
    left join parents p 
        on c.parent1_id = p.parent_id
    left join groups g 
        on c.group_id = g.group_id
),

child_lead as (
    select 
        cd.child_id,
        cd.group_name, 
        date(cd.date_created) as date_created,
        date(cd.date_started) as date_started, 
        date(cd.ended_at_clean) as ended_at_clean,
        date(cs.date_updated) as date_updated,
        cs.group_status,
        case 
            when date(ended_at_clean) <= date(date_updated) then date(ended_at_clean)
            when date(date_updated) <= date(ended_at_clean) then date(date_updated)
            else date(ended_at_clean) 
        end as ended_at_perso,
        cd.departement,
        cd.age_at_registration,
        cd.child_status
    from child_data cd 
    left join change_status cs
        on cd.child_id = cs.item_id
    where is_excluded_from_analytics = false 
        or is_excluded_from_analytics is null
)

select 
    -- filters
    s.channel as source_channel,
    s.name as source_name,
    p.postal_code as family_postal_code,
    p.city_name as family_city,
    date_trunc('year', cpf.date_created) as year_of_registration,
    substr(p.postal_code, 1, 2) as departement,

    -- main fields
    cpf.child_id,
    cpf.is_a_father,
    date_trunc('month', cpf.date_created) as month_of_registration,
    cpf.date_created as date_of_registration,

    -- age category
    case 
        when cl.age_at_registration < 3 then 'A/ <3 mois'
        when cl.age_at_registration <= 12 then 'B/ 3-12 mois'
        when cl.age_at_registration <= 18 then 'C/ 12-18 mois'
        when cl.age_at_registration <= 24 then 'D/ 18-24 mois'
        when cl.age_at_registration <= 30 then 'E/ 24-30 mois'
        when cl.age_at_registration > 30 then 'F/ >30 mois'
        else 'G/ NSP' 
    end as child_age_in_month,

    -- accompagnement indicators
    case 
        when ended_at_perso >= date_trunc('year', current_date - interval '2 year') 
            and date_started <= date_trunc('year', current_date - interval '2 year') then 1
        when extract(year from date_started) = extract(year from current_date - interval '2 year') then 1
        when extract(year from cl.date_created) = extract(year from current_date - interval '2 year') 
            and (extract(year from date_started) = extract(year from current_date - interval '1 year')) then 1
        when extract(year from cl.date_created) = extract(year from current_date - interval '2 year') 
            and child_status = 'waiting' then 1
        else 0 
    end as accompagnement_annee_n_moins_2,

    case 
        when ended_at_perso >= date_trunc('year', current_date - interval '1 year') 
            and date_started <= date_trunc('year', current_date - interval '1 year') then 1
        when extract(year from date_started) = extract(year from current_date - interval '1 year') then 1
        when extract(year from cl.date_created) = extract(year from current_date - interval '1 year') 
            and (extract(year from date_started) = extract(year from current_date)) then 1
        when extract(year from cl.date_created) = extract(year from current_date - interval '1 year') 
            and child_status = 'waiting' then 1
        else 0 
    end as accompagnement_annee_n_moins_1,

    case 
        when ended_at_perso >= date_trunc('year', current_date - interval '1 year') 
            and date_started <= date_trunc('year', current_date - interval '1 year') 
            then 'Enfant accompagne 1 janvier'
        when extract(year from date_started) = extract(year from current_date - interval '1 year') 
            then 'Enfant ayant commence son accompagnement dans annee'
        when extract(year from cl.date_created) = extract(year from current_date - interval '1 year') 
            and (extract(year from date_started) = extract(year from current_date)) 
            then 'Enfant inscrits dont accompagnement pas commence'
        when extract(year from cl.date_created) = extract(year from current_date - interval '1 year') 
            and child_status = 'waiting' 
            then 'Enfant inscrits dont accompagnement pas commence et pas planifié'
        else null 
    end as accompagnement_annee_n_moins_1_decompose,

    case 
        when ended_at_perso >= date_trunc('year', current_date) 
            and date_started <= date_trunc('year', current_date) then 1
        when extract(year from date_started) = extract(year from current_date) then 1
        when extract(year from cl.date_created) = extract(year from current_date) 
            and (extract(year from date_started) = extract(year from current_date + interval '1 year')) then 1
        when extract(year from cl.date_created) = extract(year from current_date) 
            and child_status = 'waiting' then 1
        else 0 
    end as accompagnement_annee_n,

    case 
        when ended_at_perso >= date_trunc('year', current_date) 
            and date_started <= date_trunc('year', current_date) 
            then 'Enfant accompagne 1 janvier'
        when extract(year from date_started) = extract(year from current_date) 
            then 'Enfant ayant commence son accompagnement dans annee'
        when extract(year from cl.date_created) = extract(year from current_date) 
            and (extract(year from date_started) = extract(year from current_date + interval '1 year')) 
            then 'Enfant inscrits dont accompagnement pas commence'
        when extract(year from cl.date_created) = extract(year from current_date) 
            and child_status = 'waiting' 
            then 'Enfant inscrits dont accompagnement pas commence et pas planifié'
        else null 
    end as accompagnement_annee_n_decompose,

    case 
        when ended_at_perso >= date_trunc('year', current_date + interval '1 year') 
            and date_started <= date_trunc('year', current_date + interval '1 year') then 1
        when extract(year from date_started) = extract(year from current_date + interval '1 year') then 1
        when extract(year from cl.date_created) = extract(year from current_date + interval '1 year') then 1
        else 0 
    end as accompagnement_annee_n_1,

    case 
        -- cohort with less than 3 months at the end of current year
        when (date_part('year', (date_trunc('year', current_date + interval '1 year') - interval '1 day')) - 
              date_part('year', date_started)) * 12 + 
             (date_part('month', (date_trunc('year', current_date + interval '1 year') - interval '1 day')) - 
              date_part('month', date_started)) < 3
        then 1
        
        -- cohort with less than 6 months today, will be between 6-11 months at year end
        when (date_part('year', (date_trunc('year', current_date + interval '1 year') - interval '1 day')) - 
              date_part('year', date_started)) * 12 + 
             (date_part('month', (date_trunc('year', current_date + interval '1 year') - interval '1 day')) - 
              date_part('month', date_started)) between 6 and 11
            and (date_part('year', current_date) - date_part('year', date_started)) * 12 + 
                (date_part('month', current_date) - date_part('month', date_started)) < 6 
        then 0.8
        
        -- cohort with less than 6 months today, will be between 3-11 months at year end
        when (date_part('year', (date_trunc('year', current_date + interval '1 year') - interval '1 day')) - 
              date_part('year', date_started)) * 12 + 
             (date_part('month', (date_trunc('year', current_date + interval '1 year') - interval '1 day')) - 
              date_part('month', date_started)) between 3 and 11
            and (date_part('year', current_date) - date_part('year', date_started)) * 12 + 
                (date_part('month', current_date) - date_part('month', date_started)) < 6 
        then 0.6

        -- cohort with more than 6 months today and end date after year end
        when (date_part('year', current_date) - date_part('year', date_started)) * 12 + 
             (date_part('month', current_date) - date_part('month', date_started)) >= 6
            and (date_part('year', (date_trunc('year', current_date + interval '1 year') - interval '1 day')) - 
                 date_part('year', ended_at_perso)) * 12 + 
                (date_part('month', (date_trunc('year', current_date + interval '1 year') - interval '1 day')) - 
                 date_part('month', ended_at_perso)) <= 0
        then 1 
        
        else 0 
    end as accompagnement_annee_n_1_ajuste,

    case 
        -- cohort with less than 3 months at the end of current year
        when (date_part('year', (date_trunc('year', current_date + interval '1 year') - interval '1 day')) - 
              date_part('year', date_started)) * 12 + 
             (date_part('month', (date_trunc('year', current_date + interval '1 year') - interval '1 day')) - 
              date_part('month', date_started)) < 3
        then 1
        
        -- cohort with less than 6 months today, will be between 6-11 months at year end
        when (date_part('year', (date_trunc('year', current_date + interval '1 year') - interval '1 day')) - 
              date_part('year', date_started)) * 12 + 
             (date_part('month', (date_trunc('year', current_date + interval '1 year') - interval '1 day')) - 
              date_part('month', date_started)) between 6 and 11
            and (date_part('year', current_date) - date_part('year', date_started)) * 12 + 
                (date_part('month', current_date) - date_part('month', date_started)) < 6 
        then 0.8
        
        -- cohort with less than 6 months today, will be between 3-11 months at year end
        when (date_part('year', (date_trunc('year', current_date + interval '1 year') - interval '1 day')) - 
              date_part('year', date_started)) * 12 + 
             (date_part('month', (date_trunc('year', current_date + interval '1 year') - interval '1 day')) - 
              date_part('month', date_started)) between 3 and 11
            and (date_part('year', current_date) - date_part('year', date_started)) * 12 + 
                (date_part('month', current_date) - date_part('month', date_started)) < 6 
        then 0.6

        -- cohort with more than 6 months today and end date after year end
        when (date_part('year', current_date) - date_part('year', date_started)) * 12 + 
             (date_part('month', current_date) - date_part('month', date_started)) >= 6
            and (date_part('year', (date_trunc('year', current_date + interval '1 year') - interval '1 day')) - 
                 date_part('year', ended_at_perso)) * 12 + 
                (date_part('month', (date_trunc('year', current_date + interval '1 year') - interval '1 day')) - 
                 date_part('month', ended_at_perso)) <= 0
        then 1 
        
        else 0 
    end as accompagnement_annee_n_1_ajuste_decompose

from child_lead cl 
left join source s 
    on cl.child_id = s.child_id 
left join child_parent_family cpf 
    on cpf.child_id = cl.child_id
left join parents p 
    on p.parent_id = cpf.parent1_id
