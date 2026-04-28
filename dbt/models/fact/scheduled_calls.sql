with scheduled_calls as (
    select * from {{ ref('stg_1001mots_app__scheduled_calls') }}
),

admin_users as (
    select * from {{ ref('stg_1001mots_app__admin_users') }}
),

child_supports as (
    select * from {{ ref('stg_1001mots_app__child_supports') }}
),

groups as (
    select * from {{ ref('groups') }}
),

child as (
    select * from {{ ref('child') }}
)

select
    sc.scheduled_call_id,
    sc.family_id,
    sc.parent_id,
    sc.admin_user_id,
    au.name                                             as supporter_name,
    sc.call_session,
    g.group_name                                        as cohort_name,
    g.is_excluded_from_analytics,
    c.child_id,
    c.gender,
    c.date_birth                                        as birthdate,
    sc.date_scheduled,
    sc.duration_minutes,
    sc.event_type_name,
    sc.event_type_uri,
    sc.calendly_event_uri,
    sc.calendly_invitee_uri,
    sc.invitee_email,
    sc.invitee_name,
    sc.invitee_comment,
    sc.status,
    sc.date_canceled,
    sc.cancellation_reason,
    sc.cancel_url,
    sc.date_created,
    sc.date_updated
from scheduled_calls as sc
left join admin_users as au
    on au.supporter_id = sc.admin_user_id
left join child_supports as cs
    on cs.family_id = sc.family_id
left join child as c
    on c.family_id = sc.family_id
left join groups as g
    on g.group_id = c.group_id
