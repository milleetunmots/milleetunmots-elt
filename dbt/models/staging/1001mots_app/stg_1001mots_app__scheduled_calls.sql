with source as (
    select *
    from {{ source('mots_app', 'scheduled_calls') }}
)

select
    id::string                                          as scheduled_call_id,
    admin_user_id::string                               as admin_user_id,
    child_support_id::string                            as family_id,
    parent_id::string                                   as parent_id,
    call_session::integer                               as call_session,
    calendly_event_uri::string                          as calendly_event_uri,
    calendly_invitee_uri::string                        as calendly_invitee_uri,
    scheduled_at::timestamp_ntz                         as scheduled_at,
    to_date(
            nullif(scheduled_at::string, '')
    ) as date_scheduled,
    canceled_at::timestamp_ntz                          as canceled_at,
    duration_minutes::integer                           as duration_minutes,
    event_type_name::string                             as event_type_name,
    event_type_uri::string                              as event_type_uri,
    invitee_email::string                               as invitee_email,
    invitee_name::string                                as invitee_name,
    invitee_comment::string                             as invitee_comment,
    status::string                                      as status,
    to_date(
            nullif(canceled_at::string, '')
    ) as date_canceled,
    cancellation_reason::string                         as cancellation_reason,
    cancel_url::string                                  as cancel_url,
    parse_json(raw_payload)                             as raw_payload,
    to_date(
            nullif(created_at::string, '')
    ) as date_created,
    to_date(
            nullif(updated_at::string, '')
    ) as date_updated
from source
