with c as (
    select *
    from {{ ref('stg_1001mots_app__children') }}
)

select
    child_id,
    parent1_id,
    parent2_id,
    family_id,
    group_id,
    date_group_end,
    first_name,
    last_name,
    date_birth,
    date_created,
    (DATE_PART('year', CURRENT_DATE) - DATE_PART('year', date_birth)) * 12 + (DATE_PART('month', CURRENT_DATE) - DATE_PART('month', date_birth)) AS ages,
    (date_part('year', date_created) - date_part('year', date_birth)) * 12 + 
        (date_part('month', date_created) - date_part('month', date_birth)) as age_at_registration,
    {{ compute_age_in_month('age_at_registration') }} as child_age_in_month,
    date_updated,
    date_discarded,
    gender,
    should_contact_parent1,
    should_contact_parent2,
    registration_source_details,
    registration_source,
    family_redirection_urls_count,
    family_redirection_url_visits_count,
    group_status
from c