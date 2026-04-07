with pe as (
    select *
    from {{ ref('parent_enfant') }}
),

a as (
    select *
    from {{ ref('atelier') }}
),

ast as (
    select *
    from {{ ref('atelier_stat') }}
)

select distinct
    pe.parent_id,
    a.family_id,
    concat(lower(pe.parent_first_name), ' ', lower(pe.parent_last_name)) as parent_name,
    pe.phone_number,
    concat(lower(pe.parent_first_name), ' ', lower(pe.parent_last_name), ' (', pe.phone_number, ')') as parent_name_and_phone,
    iff(pe.follow_us_on_whatsapp, 'Oui', 'Non') as present_on_whatsapp,
    pe.group_name,
    concat('https://app.1001mots.org/admin/child_supports/', pe.child_support_id) as child_support_url,
    pe.registration_source,
    pe.registration_source_details,
    pe.accompagnante_email,
    pe.accompagnante_name,
    pe.accompagnante_role,
    pe.accompagnante_aircall_phone_number,
    concat(pe.accompagnante_name, ' (', pe.accompagnante_aircall_phone_number, ') - ', pe.group_name) as accompagnante_name_and_phone_and_cohort,
    pe.children_names_and_ages,
    a.workshop_id,
    a.workshop_topic,
    a.workshop_year,
    a.workshop_month,
    a.date_discarded,
    a.date_workshop,
    a.workshop_postal_code,
    a.workshop_city_name,
    a.workshop_name,
    concat(a.workshop_name, ' (', coalesce(a.time_slot, 'seul horaire'), ')') as workshop_name_and_time_slot,
    a.time_slot,
    a.workshop_land,
    a.workshop_status,
    a.parent_response,
    a.parent_presence,
    --a.date_accepted,
    -- Pas de notion de date de refus / date de reponse
    to_varchar(CONVERT_TIMEZONE('UTC', 'Europe/Paris', a.date_updated), 'DD/MM/YYYY HH24:MI:SS') as date_accepted,
    a.animator_email,
    a.animator_name,
    a.animator_role,
    a.co_animator,
    ast.atelier_inscrit,
    ast.atelier_present,
    ast.atelier_present_topics,
    ast.atelier_en_attente,
    ast.atelier_absence_planifiee,
    ast.atelier_absence_non_planifiee,
    ast.atelier_inscrits_et_annule
from pe
left join a
    on pe.parent_id = a.related_id
left join ast
    on pe.parent_id = ast.related_id
where workshop_id is not null
and a.animator_role = 'animator'
--and coalesce(a.parent_response, '') != 'Non'
-- Remove the workshops that have been discarded
and date_discarded is null
--and not a.animator_is_disabled