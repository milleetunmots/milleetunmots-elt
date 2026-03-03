with events as (
    select *
    from {{ ref('stg_1001mots_app__events') }}
    where type = 'Events::TextMessage'
    and spot_hit_status in ('1','2')
),

pe as (
    select *
    from {{ ref('parent_enfant') }}
),

mecene as (
    select *
    from {{ ref('mecene') }}
)

select
    mecene.*,
    events.date_occurred,
    events.body
from events
left join pe
    on events.related_id = pe.parent_id
left join mecene
    on pe.youngest_child_id = mecene.child_id
