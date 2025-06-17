with admin_users as (
    select *
    from {{ ref('stg_1001mots_app__admin_users') }}
),

child_supports as (
    select *
    from {{ ref('stg_1001mots_app__child_supports') }}
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
)


select 
    c.date_created,
    c.date_discarded,
    c.child_id,
    p1.parent_id as parent1_id, 
    p2.parent_id as parent2_id,
    cs.family_id,
    au.supporter_id,
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
    on c.family_id = cs.family_id
left join groups g
    on c.group_id = g.group_id
left join admin_users au
    on au.supporter_id = cs.supporter_id
--where c.date_discarded is null