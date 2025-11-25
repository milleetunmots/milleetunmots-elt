with w as (
	select
		workshop_id,
		topic,
		co_animator,
		animator_id,
        date_discarded,
		date_workshop,
		postal_code as workshop_postal_code,
		city_name as workshop_city_name,
		workshop_name,
		workshop_land
	from {{ ref('stg_1001mots_app__workshops') }}
),

e as (
	select
		workshop_id,
		related_type,
		related_id,
        date_accepted,
		date_created,
		date_occurred,
		body,
		parent_response,
		parent_presence
	from {{ ref('stg_1001mots_app__events') }}
	where workshop_id is not null
	and date_discarded is null
	and related_type = 'Parent'
	and (parent_presence is not null or parent_response is not null)
),

au as (
	select
		supporter_id,
		email as supporter_email,
		name as supporter_name,
		user_role,
		is_disabled
	from {{ ref('admin_users') }}
),

cal as (
    select *
    from {{ ref('calendar') }}
)

select
	w.workshop_id,
	w.topic,
	w.co_animator,
    cal.year as workshop_year,
    cal.month as workshop_month,
	w.date_discarded,
    w.date_workshop,
	w.workshop_postal_code,
	w.workshop_city_name,
	w.workshop_name,
	w.workshop_land,
	e.parent_response,
	e.parent_presence,
	e.date_accepted,
	e.related_id,
	au.supporter_email as animator_email,
	au.supporter_name as animator_name,
	au.user_role as animator_role,
    case
        when w.date_discarded is not null then 'Annulé'
        when w.date_workshop < current_date then 'Passé'
        when w.date_workshop >= current_date then 'Planifié'
    end as workshop_status
from w
left join e
	on w.workshop_id = e.workshop_id
left join au
	on w.animator_id = au.supporter_id
left join cal
    on w.date_workshop = cal.full_date