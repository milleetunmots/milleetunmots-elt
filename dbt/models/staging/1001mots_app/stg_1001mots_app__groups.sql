with source as (
    select *
    from {{ source('1001mots_app', 'groups') }}
)

select
    id::string as group_id,
    name::string as group_name,
    to_date(
            nullif(created_at::string, '')
    ) as date_created,
    to_date(
            nullif(started_at::string, '')
    ) as date_started,
    to_date(
            nullif(ended_at::string, '')
    ) as date_ended,
    to_date(
            nullif(updated_at::string, '')
    ) as date_updated,
    to_date(
            nullif(discarded_at::string, '')
    ) as date_discarded,
    to_date(
            nullif(call0_start_date::string, '')
    ) as date_call0_start,
    to_date(
            nullif(call0_end_date::string, '')
    ) as date_call0_end,
    to_date(
            nullif(call1_start_date::string, '')
    ) as date_call1_start,
    to_date(
            nullif(call1_end_date::string, '')
    ) as date_call1_end,
    to_date(
            nullif(call2_start_date::string, '')
    ) as date_call2_start,
    to_date(
            nullif(call2_end_date::string, '')
    ) as date_call2_end,
    to_date(
            nullif(call3_start_date::string, '')
    ) as date_call3_start,
    to_date(
            nullif(call3_end_date::string, '')
    ) as date_call3_end,
    is_programmed::boolean as is_programmed,
    is_excluded_from_analytics::boolean as is_excluded_from_analytics
    --Ajouter les autres colonnes
from source