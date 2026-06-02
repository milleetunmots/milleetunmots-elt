with source as (
    select *
    from {{ source('mots_app', 'tasks') }}
)

select
    id::integer as task_id,
    reporter_id::integer as reporter_id,
    assignee_id::integer as assignee_id,
    related_type::string as related_type,
    related_id::integer as related_id,
    title::string as title,
    description::string as description,
    to_date(
        nullif(due_date::string, '')
    ) as date_due,
    to_date(
        nullif(done_at::string, '')
    ) as date_done,
    to_date(
        nullif(created_at::string, '')
    ) as date_created,
    to_timestamp(
        nullif(updated_at::string, '')
    ) as date_updated,
    to_date(
        nullif(discarded_at::string, '')
    ) as date_discarded,
    status::string as status,
    treated_by_id::integer as treated_by_id
from source
