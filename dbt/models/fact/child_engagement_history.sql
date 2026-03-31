with children as (
    select * from {{ ref('child') }}
),

groups as (
    select * from {{ ref('groups') }}
),

family as (
    select * from {{ ref('family') }}
    where date_discarded is null
),

taggings as (
    select * from {{ ref('stg_1001mots_app__taggings') }}
),

groups_info as (
    select
        group_id,
        group_name,
        date_started::date      as date_started,
        date_call0_end::date    as date_call0_end,
        date_call1_end::date    as date_call1_end,
        date_call2_end::date    as date_call2_end,
        date_call3_end::date    as date_call3_end,
        date_ended_clean::date  as date_ended
    from groups
),

eval_t1 as (
    select
        taggable_id as family_id,
        case
            when max(case when tag_id = '901' then 1 else 0 end) = 1 then 'Désengagé T1'
            when max(case when tag_id = '900' then 1 else 0 end) = 1 then 'Estimé désengagé T1 conservé'
            when max(case when tag_id = '893' then 1 else 0 end) = 1 then 'Estimé désengagé T1'
            else 'Conservé T1'
        end as engagement_state
    from taggings
    where taggable_type = 'ChildSupport'
        and tag_id in ('901', '900', '893')
    group by taggable_id
),

eval_t2 as (
    select
        taggable_id as family_id,
        case
            when max(case when tag_id = '876' then 1 else 0 end) = 1 then 'Désengagé T2'
            when max(case when tag_id = '874' then 1 else 0 end) = 1 then 'Estimé désengagé T2'
            when max(case when tag_id = '877' then 1 else 0 end) = 1 then 'Estimé désengagé T2 conservé'
            else 'Conservé T2'
        end as engagement_state
    from taggings
    where taggable_type = 'ChildSupport'
        and tag_id in ('876', '874', '877')
    group by taggable_id
),

punctual_tags as (
    select
        taggable_id as family_id,
        tag_id,
        date_created::date as event_date
    from taggings
    where taggable_type = 'ChildSupport'
        and tag_id in ('1071', '1076')
),

all_events as (

    select
        c.child_id,
        f.family_id,
        c.date_created::date        as event_date,
        'Inscription'               as engagement_step,
        'En attente'                as engagement_state,
        null                        as call_number,
        g.group_name                as cohort_name
    from children c
    inner join family f         on f.child_id = c.child_id
    inner join groups_info g    on g.group_id = c.group_id
    where c.date_discarded is null

    union all

    select
        c.child_id,
        f.family_id,
        g.date_started              as event_date,
        'Accompagnement démarré'    as engagement_step,
        'Actif'                     as engagement_state,
        null                        as call_number,
        g.group_name                as cohort_name
    from children c
    inner join family f         on f.child_id = c.child_id
    inner join groups_info g    on g.group_id = c.group_id
    where c.date_discarded is null
        and g.date_started is not null

    union all

    select
        c.child_id,
        f.family_id,
        g.date_call0_end            as event_date,
        'Après call 0'              as engagement_step,
        'Actif'                     as engagement_state,
        '0'                         as call_number,
        g.group_name                as cohort_name
    from children c
    inner join family f         on f.child_id = c.child_id
    inner join groups_info g    on g.group_id = c.group_id
    where c.date_discarded is null
        and g.date_call0_end is not null

    union all

    select
        c.child_id,
        f.family_id,
        g.date_call1_end                                as event_date,
        'Évaluation T1 (après call 1)'                  as engagement_step,
        coalesce(e1.engagement_state, 'Conservé T1')    as engagement_state,
        '1'                                             as call_number,
        g.group_name                                    as cohort_name
    from children c
    inner join family f         on f.child_id = c.child_id
    inner join groups_info g    on g.group_id = c.group_id
    left join eval_t1 e1        on e1.family_id = f.family_id
    where c.date_discarded is null
        and g.date_call1_end is not null

    union all

    select
        c.child_id,
        f.family_id,
        g.date_call2_end                                as event_date,
        'Évaluation T2 (après call 2)'                  as engagement_step,
        coalesce(e2.engagement_state, 'Conservé T2')    as engagement_state,
        '2'                                             as call_number,
        g.group_name                                    as cohort_name
    from children c
    inner join family f         on f.child_id = c.child_id
    inner join groups_info g    on g.group_id = c.group_id
    left join eval_t2 e2        on e2.family_id = f.family_id
    where c.date_discarded is null
        and g.date_call2_end is not null

    union all

    select
        c.child_id,
        f.family_id,
        g.date_call3_end            as event_date,
        'Après call 3'              as engagement_step,
        'Actif'                     as engagement_state,
        '3'                         as call_number,
        g.group_name                as cohort_name
    from children c
    inner join family f         on f.child_id = c.child_id
    inner join groups_info g    on g.group_id = c.group_id
    where c.date_discarded is null
        and g.date_call3_end is not null

    union all

    select
        c.child_id,
        f.family_id,
        pt.event_date               as event_date,
        'Désengagé (2 appels KO)'   as engagement_step,
        'Désengagé'                 as engagement_state,
        null                        as call_number,
        g.group_name                as cohort_name
    from children c
    inner join family f         on f.child_id = c.child_id
    inner join groups_info g    on g.group_id = c.group_id
    inner join punctual_tags pt on pt.family_id = f.family_id
        and pt.tag_id = '1071'
    where c.date_discarded is null

    union all

    select
        c.child_id,
        f.family_id,
        pt.event_date               as event_date,
        'Accompagnement redémarré'  as engagement_step,
        'Actif'                     as engagement_state,
        null                        as call_number,
        g.group_name                as cohort_name
    from children c
    inner join family f         on f.child_id = c.child_id
    inner join groups_info g    on g.group_id = c.group_id
    inner join punctual_tags pt on pt.family_id = f.family_id
        and pt.tag_id = '1076'
    where c.date_discarded is null

)

select
    child_id,
    family_id,
    cohort_name,
    case engagement_step
        when 'Inscription'                  then 1
        when 'Accompagnement démarré'       then 2
        when 'Après call 0'                 then 3
        when 'Évaluation T1 (après call 1)' then 4
        when 'Évaluation T2 (après call 2)' then 5
        when 'Après call 3'                 then 6
        when 'Désengagé (2 appels KO)'      then 7
        when 'Accompagnement redémarré'     then 8
    end                                                             as canonical_step,
    event_date                                                      as date_from,
    greatest(
        event_date,
        coalesce(
            lead(event_date) over (partition by child_id order by event_date),
            current_date
        )
    )                                                               as date_to,
    engagement_step,
    engagement_state,
    call_number
from all_events
order by child_id, event_date
