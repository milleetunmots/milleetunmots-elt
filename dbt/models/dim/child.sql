with c as (
    select *
    from {{ ref('stg_1001mots_app__children') }}
),

children_sources as (
    select *
    from {{ ref('stg_1001mots_app__children_sources') }}
),

sources as (
    select *
    from {{ ref('stg_1001mots_app__sources') }}
),

source as (
    select 
        css.child_id,
        css.source_id,
        s.name, 
        s.channel, 
        s.department
    from children_sources css
    inner join sources s
        on css.source_id = s.source_id
)

select
    c.child_id,
    c.parent1_id,
    c.parent2_id,
    c.family_id,
    c.group_id,
    date_group_end,
    c.first_name,
    c.last_name,
    date_birth,
    c.date_created,
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
    source.name as registration_source_name,
    source.channel as registration_source_channel,
    source.department as registration_source_department,
    family_redirection_urls_count,
    family_redirection_url_visits_count,
    group_status
from c
left join source
    on c.child_id = source.child_id