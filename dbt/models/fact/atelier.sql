with w as (
	select
		workshop_id,
		topic,
		co_animator,
		animator_id,
        date_discarded,
		date_workshop,
		first_workshop_time_slot as time_slot,
		'1' as time_slot_number,
		postal_code as workshop_postal_code,
		city_name as workshop_city_name,
		workshop_name,
		workshop_land
	from {{ ref('stg_1001mots_app__workshops') }}
	union all
	select
		workshop_id,
		topic,
		co_animator,
		animator_id,
        date_discarded,
		date_workshop,
		second_workshop_time_slot as time_slot,
		'2' as time_slot_number,
		postal_code as workshop_postal_code,
		city_name as workshop_city_name,
		workshop_name,
		workshop_land
	from {{ ref('stg_1001mots_app__workshops') }}
	where second_workshop_time_slot is not null
),

e as (
	select
		workshop_id,
		related_type,
		related_id,
        date_accepted,
		date_created,
		date_occurred,
        date_updated,
		body,
		parent_response,
		parent_presence,
		workshop_time_slot
	from {{ ref('stg_1001mots_app__events') }}
	where workshop_id is not null
	and date_discarded is null
	and related_type = 'Parent'
	--and (parent_presence is not null or parent_response is not null)
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
	case w.topic
        when 'games' then 'Jeux de recup'
        when 'sleep' then 'Coucher'
        when 'emotion' then 'Émotions'
        when 'bath' then 'Bain / Habillage / Change'
        when 'outside' then 'Sorties'
        when 'meal' then 'Repas'
        when 'books' then 'Livres'
        when 'nursery_rhymes' then 'Comptines'
        else w.topic
    end as workshop_topic,
	w.co_animator,
    cal.year as workshop_year,
    cal.month as workshop_month,
	w.date_discarded,
    w.date_workshop,
	w.time_slot,
	w.workshop_postal_code,
	w.workshop_city_name,
	w.workshop_name,
	w.workshop_land,
	iff(e.parent_response is not null, iff(e.workshop_time_slot = w.time_slot_number or w.time_slot_number is null, e.parent_response, 'Non'), null) as parent_response,
	iff(
		e.parent_presence is not null and (e.workshop_time_slot = w.time_slot_number or  w.time_slot_number is null),
		case e.parent_presence
			when 'present' then 'Présent'
			when 'queue' then 'En attente'
			when 'planned_absence' then 'Absence planifiée'
			when 'not_planned_absence' then 'Absence non planifiée'
			else e.parent_presence
		end,
		null)
	as parent_presence,
	iff(e.date_accepted is not null and (e.workshop_time_slot = w.time_slot_number or  w.time_slot_number is null), e.date_accepted, null) as date_accepted,
	iff(e.date_updated is not null and (e.workshop_time_slot = w.time_slot_number or  w.time_slot_number is null), e.date_updated, null) as date_updated,
	e.related_id,
	au.supporter_email as animator_email,
	au.supporter_name as animator_name,
	au.user_role as animator_role,
    au.is_disabled as animator_is_disabled,
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