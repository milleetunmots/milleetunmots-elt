with p as (
	select
		parent_id,
		first_name as parent_first_name,
		last_name as parent_last_name,
		phone_number,
		present_on_whatsapp
	from {{ ref('parent') }}
),

au as (
	select
		supporter_id,
		email as supporter_email,
		name as supporter_name,
		user_role,
		is_disabled,
		aircall_phone_number
	from {{ ref('admin_users') }}
),

g as (
	select
		group_id,
		group_name
	from {{ ref('groups') }}
),

cs as (
	select
		family_id as child_support_id,
		supporter_id
	from {{ ref('stg_1001mots_app__child_supports') }}
),

c as (
	select
		ch.child_id,
		ch.parent1_id,
		ch.parent2_id,
        ch.date_birth,
		ch.first_name as child_first_name,
		ch.last_name as child_last_name,
		ch.family_id as child_support_id,
		ch.group_id,
		-- Pas très clean, il faudra&it distinguer les 2
		ch.registration_source_channel as registration_source,
		ch.registration_source_details,
		ch.ages as age_in_months,
        max_birthdate.youngest_child_id as youngest_child_id
	from {{ ref('child') }} as ch
    inner join (
        SELECT
            family_id, 
            MAX(date_birth) AS youngest_child_birthdate,
            MAX(child_id) as youngest_child_id
        FROM {{ ref('child') }} as children
        inner join {{ ref('groups') }} as gr
            on children.group_id = gr.group_id
        --where (gr.is_excluded_from_analytics = false 
        --    or (gr.is_excluded_from_analytics is null and children.group_status != 'not_supported'))
        --and children.date_discarded is null
        GROUP BY family_id
    ) AS max_birthdate
    ON max_birthdate.family_id = ch.family_id 
)

select
	p.parent_id,
	p.parent_first_name,
	p.parent_last_name,
	p.phone_number,
	p.present_on_whatsapp,
	--c.child_first_name,
	--c.child_Last_name,
	--c.registration_source,
	--c.registration_source_details,
	--c.age_in_months,
	--c.child_support_id,
    cs.child_support_id,
    case c1.registration_source
        when 'caf' then 'caf'
        when 'pmi' then 'pmi'
        when 'resubscribing' then 'Réinscription'
        when 'friends' then 'Amis'
        when 'other' then 'Autres'
        else c1.registration_source
    end as registration_source,
    c1.registration_source_details,
	g.group_name,
	au.supporter_email as accompagnante_email,
	au.supporter_name as accompagnante_name,
	au.user_role as accompagnante_role,
    au.aircall_phone_number as accompagnante_aircall_phone_number,
    arrayagg(concat(lower(c.child_first_name), ' ', c.age_in_months, 'mois', ' (', c.date_birth, ')')) within group (order by c.date_birth desc) as children_names_and_ages
from p
left join c
	on p.parent_id = c.parent1_id
	or p.parent_id = c.parent2_id
left join c as c1
	on c.youngest_child_id = c1.child_id
left join g
	on c1.group_id = g.group_id
left join cs
	on c1.child_support_id  = cs.child_support_id
left join au
	on cs.supporter_id = au.supporter_id
group by 1,2,3,4,5,6,7,8,9,10,11,12,13