with a as (
    select * 
    from {{ ref('atelier') }}
    where date_workshop < current_date
    and (parent_response = 'Oui' or parent_presence is not null)
),

stat1 as (
    select
        related_id,
        arrayagg(concat(workshop_city_name, '_', workshop_year, '_', workshop_month)) within group (order by date_workshop desc) as atelier_inscrit
    from a
    where parent_response = 'Oui'
    group by 1
),

stat2 as (
    select
        related_id,
        arrayagg(concat(workshop_city_name, '_', workshop_year, '_', workshop_month)) within group (order by date_workshop desc) as atelier_present
    from a
    where parent_presence = 'present'
    group by 1
),

stat3 as (
    select
        related_id,
        arrayagg(concat(workshop_city_name, '_', workshop_year, '_', workshop_month)) within group (order by date_workshop desc) as atelier_en_attente
    from a
    where parent_presence = 'queue'
    group by 1
),

stat4 as (
    select
        related_id,
        arrayagg(concat(workshop_city_name, '_', workshop_year, '_', workshop_month)) within group (order by date_workshop desc) as atelier_absence_planifiee
    from a
    where parent_presence = 'planned_absence'
    group by 1
),

stat5 as (
    select
        related_id,
        arrayagg(concat(workshop_city_name, '_', workshop_year, '_', workshop_month)) within group (order by date_workshop desc) as atelier_absence_non_planifiee
    from a
    where parent_presence = 'not_planned_absence'
    group by 1
),

stat6 as (
    select
        related_id,
        arrayagg(concat(workshop_city_name, '_', workshop_year, '_', workshop_month)) within group (order by date_workshop desc) as atelier_inscrits_et_annule
    from a
    where parent_response = 'Oui'
    and date_discarded is not null
    group by 1
)

select
    stat1.related_id,
    stat1.atelier_inscrit,
    stat2.atelier_present,
    stat3.atelier_en_attente,
    stat4.atelier_absence_planifiee,
    stat5.atelier_absence_non_planifiee,
    stat6.atelier_inscrits_et_annule
from stat1
left join stat2
    on stat1.related_id = stat2.related_id
left join stat3
    on stat1.related_id = stat3.related_id
left join stat4
    on stat1.related_id = stat4.related_id
left join stat5
    on stat1.related_id = stat5.related_id
left join stat6
    on stat1.related_id = stat6.related_id