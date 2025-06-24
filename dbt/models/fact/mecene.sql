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
    from {{ ref('child') }}
),

parents as (
    select *
    from {{ ref('parent') }}
),

groups as (
    select *
    from {{ ref('groups') }}
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
    select *
    from {{ ref('family') }}
    where date_discarded is null
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

change_status as (
    select 
        item_id,
        parse_json(object_changes):"date_updated"[0] as date_updated,
        parse_json(object_changes):"group_status"[1] as group_status
    from children c 
    left join versions v
        on v.item_id = c.child_id
    where item_type = 'Child' 
        and parse_json(object_changes):"group_status"[1] in ('stopped', 'disengaged')
),

child_data as (
    select 
        c.child_id, 
        c.date_created,
        g.group_name,
        g.is_excluded_from_analytics,
        g.date_started, 
        g.date_ended_clean as ended_at_clean,
        p.departement,
        c.date_birth as birthdate,
        c.age_at_registration,
        c.child_age_in_month,
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
        cd.child_age_in_month,
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
    p.departement,

    -- main fields
    cpf.child_id,
    cpf.is_a_father,
    date_trunc('month', cpf.date_created) as month_of_registration,
    cpf.date_created as date_of_registration,

    -- age category
    child_age_in_month,

    -- accompagnement indicators
    
    -- accompagnement indicators N-2
    cl.date_started as cl_date_started,
    cl.ended_at_perso as cl_ended_at_perso,
    cl.date_created as cl_date_created,
    {{ accompagnement_annee_n_moins_2('cl.date_started', 'cl.ended_at_perso', 'cl.date_created', 'cl.child_status') }} as accompagnement_annee_n_moins_2,
    
    -- accompagnement indicators N-1
    {{ accompagnement_annee_n_moins_1('cl.date_started', 'cl.ended_at_perso', 'cl.date_created', 'cl.child_status') }} as accompagnement_annee_n_moins_1,
    {{ accompagnement_annee_n_moins_1_decompose('cl.date_started', 'cl.ended_at_perso', 'cl.date_created', 'cl.child_status') }} as accompagnement_annee_n_moins_1_decompose,
    
    -- accompagnement indicators N
    {{ accompagnement_annee_n('cl.date_started', 'cl.ended_at_perso', 'cl.date_created', 'cl.child_status') }} as accompagnement_annee_n,
    {{ accompagnement_annee_n_decompose('cl.date_started', 'cl.ended_at_perso', 'cl.date_created', 'cl.child_status') }} as accompagnement_annee_n_decompose,

    -- accompagnement indicators N+1
    {{ accompagnement_annee_n_1('cl.date_started', 'cl.ended_at_perso', 'cl.date_created') }} as accompagnement_annee_n_1,
    {{ accompagnement_annee_n_1_ajuste('cl.date_started', 'cl.ended_at_perso') }} as accompagnement_annee_n_1_ajuste,
    {{ accompagnement_annee_n_1_ajuste_decompose('cl.date_started', 'cl.ended_at_perso') }} as accompagnement_annee_n_1_ajuste_decompose,

from child_lead cl 
left join source s 
    on cl.child_id = s.child_id 
left join child_parent_family cpf 
    on cpf.child_id = cl.child_id
left join parents p 
    on p.parent_id = cpf.parent1_id
