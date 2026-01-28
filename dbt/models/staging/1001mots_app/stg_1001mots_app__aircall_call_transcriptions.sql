with source as (
    select
        *,
        PARSE_JSON(raw_transcription_payload) as transcript_json
    from {{ source('mots_app', 'aircall_calls') }}
),


transcription as (
    SELECT
        source.id as call_id,
        value:start_time::float       as start_time,
        value:end_time::float         as end_time,
        value:text::string            as text,
        value:participant_type::string as participant_type,
        value:phone_number::string    as phone_number,
        value:user_id::int            as user_id
    FROM
        source,
        LATERAL FLATTEN(input => transcript_json)
)

select
    id::string as call_id,
    child_support_id::int as child_support_id,
    parent_id::integer as parent_id,
    caller_id::string as caller_id,
    direction::string as direction,
    answered::boolean as answered,
    started_at::timestamp as started_at,
    to_date(
            nullif(started_at::string, '')
    ) as date_started,
    call_session::integer as call_session,
    duration::integer as duration,
    asset_url::string as asset_url,
    tags::variant as tags,
    transcription.start_time,
    transcription.end_time,
    transcription.text,
    transcription.participant_type,
    transcription.phone_number,
    transcription.user_id
from source
left join transcription
    on source.id = transcription.call_id

