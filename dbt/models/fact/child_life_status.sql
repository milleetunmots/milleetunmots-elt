with children as (
    select * from {{ ref('child') }}
),

versions as (
    select * from {{ ref('stg_1001mots_app__versions') }}
),

g as (
    select * from {{ ref('groups') }}
),

change_status as (
    select distinct
        item_id,
        parse_json(object_changes):"updated_at"[1]::date as date_updated,
        -- Keep only the last value for a given date
        -- Sometimes the object changes are not updated in the same order as the versions that's why we have a coalesce
        coalesce(LAST_VALUE(parse_json(object_changes):"group_status"[1]) OVER (PARTITION BY item_id, parse_json(object_changes):"updated_at"[1]::date ORDER BY parse_json(object_changes):"updated_at"[1]::timestamp), LAST_VALUE(parse_json(object):"group_status") OVER (PARTITION BY item_id, parse_json(object_changes):"updated_at"[1]::date ORDER BY parse_json(object_changes):"updated_at"[1]::timestamp))::string as group_status,
        coalesce(last_value(parse_json(object_changes):"group_id"[1]) OVER (PARTITION BY item_id, parse_json(object_changes):"updated_at"[1]::date ORDER BY parse_json(object_changes):"updated_at"[1]::timestamp), last_value(parse_json(object):"group_id") OVER (PARTITION BY item_id, parse_json(object_changes):"updated_at"[1]::date ORDER BY parse_json(object_changes):"updated_at"[1]::timestamp))::int as cohort_id
        --coalesce(parse_json(object_changes):"group_status"[1], parse_json(object):"group_status")::string as group_status,
        --coalesce(parse_json(object_changes):"group_id"[1], parse_json(object):"group_id")::int as cohort_id
    from children c 
    left join versions v
        on v.item_id = c.child_id
    -- Voir avec Marion
    -- Que faire des cas ou il y a plusieurs changement de group_status ?
    -- On utilise group_status = waiting dans les macros mais ne fait pas partie des cas détectés
    where item_type = 'Child' 
    and (parse_json(object_changes):"group_status"[1] is not null or parse_json(object_changes):"group_id"[1] is not null) --in ('stopped', 'disengaged')
    order by 1, 2 desc
)

select
    cs.item_id,
    cs.date_updated::date as date_from,
    coalesce(lag(cs.date_updated) over (partition by cs.item_id order by cs.date_updated desc), least(g.date_ended_clean, current_date)::date)::date as date_to,
    cs.group_status::string as group_status,
    cs.cohort_id,
    g.group_name as cohort_name
from change_status as cs
left join g
    on cs.cohort_id = g.group_id
order by 1, 2 desc, 3 desc