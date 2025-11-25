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

select
    pe.parent_id,
	pe.parent_first_name,
    pe.parent_last_name,
    pe.phone_number,
    pe.present_on_whatsapp,
    pe.group_name,
    pe.accompagnante_email,
    pe.accompagnante_name,
    pe.accompagnante_role,
    pe.children_names_and_ages,
    a.workshop_id,
    a.topic,
    a.workshop_year,
    a.workshop_month,
    a.date_discarded,
    a.date_workshop,
    a.workshop_postal_code,
    a.workshop_city_name,
    a.workshop_name,
    a.workshop_land,
    a.workshop_status,
    a.parent_response,
    a.parent_presence,
    a.date_accepted,
    a.animator_email,
    a.animator_name,
    a.animator_role,
    ast.atelier_inscrit,
    ast.atelier_present,
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