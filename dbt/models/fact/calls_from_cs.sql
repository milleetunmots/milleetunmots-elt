with m as (
    select *
    from {{ ref('mecene') }}
),

st as (
    select *
    from {{ ref('super_table') }}
),

g as (
    select *
    from {{ ref('groups') }}
)

-- On fait l'apporximation que les appels sont programmés le jour même
-- du début de la session d'appel de la cohorte
select
    m.*,
    iff(replace(st.call_0_status, ' ', '') != '' and st.call_0_status is not null, 1, 0) as got_call,
    iff(replace(st.call_0_status, ' ', '') = 'OK' and st.call_0_status is not null, 1, 0) as got_call_ok,
    date_call0_start as call_start_date,
    '0' as call_number
from m
left join st
    on m.family_id = st.family_id
left join g
    on m.cohort_name = g.group_name
union all
select
    m.*,
    iff(replace(st.call1_status, ' ', '') != '' and st.call1_status is not null, 1, 0) as got_call,
    iff(replace(st.call1_status, ' ', '') = 'OK' and st.call1_status is not null, 1, 0) as got_call_ok,
    date_call1_start as call_start_date,
    '1' as call_number
from m
left join st
    on m.family_id = st.family_id
left join g
    on m.cohort_name = g.group_name
union all
select
    m.*,
    iff(replace(st.call2_status, ' ', '') != '' and st.call2_status is not null, 1, 0) as got_call,
    iff(replace(st.call2_status, ' ', '') = 'OK' and st.call2_status is not null, 1, 0) as got_call_ok,
    date_call2_start as call_start_date,
    '2' as call_number
from m
left join st
    on m.family_id = st.family_id
left join g
    on m.cohort_name = g.group_name
union all
select
    m.*,
    iff(replace(st.call3_status, ' ', '') != '' and st.call3_status is not null, 1, 0) as got_call,
    iff(replace(st.call3_status, ' ', '') = 'OK' and st.call3_status is not null, 1, 0) as got_call_ok,
    date_call3_start as call_start_date,
    '3' as call_number
from m
left join st
    on m.family_id = st.family_id
left join g
    on m.cohort_name = g.group_name