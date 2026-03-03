with tmp as (
    select *
    from {{ ref('export_compta_analytique') }}
),

final as (
select
    source_compta_analytique,
    count(distinct child_id) as nb_children
from tmp
group by 1
order by 2 desc
),

final2 as (
select
    source_compta_analytique,
    nb_children/sum(nb_children) over () as ratio
from final
order by 2 desc
)

select *
from final2
order by 2 desc