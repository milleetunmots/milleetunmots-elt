with c as (
    select *
    from {{ ref('child') }}
)

-- Ne fonctionne que tant que la famille est liée à une même fiche de suivi
select
    family_id,
    child_id as related_id,
    'Child' as related_type
from c
union all
select
    family_id,
    parent1_id as related_id,
    'Parent' as related_type
from c
where parent1_id is not null
union all
select
    family_id,
    parent2_id as related_id,
    'Parent' as related_type
from c
where parent2_id is not null