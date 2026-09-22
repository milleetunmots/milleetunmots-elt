-- d01 | Pourquoi le denominateur se limite aux appels ayant un statut.
-- Croise le statut d'appel avec l'etat du parcours a la session.
-- Resultat (2026) : les familles 'active' ont TOUJOURS un statut (0 % de manquant),
-- les 'stopped' en manquent 70 % et 74 % d'entre elles ont une date d'arret
-- anterieure a la session. Les 12 % de statuts vides sont donc des parcours
-- arretes ou desengages, pas des appels oublies.
-- Compter toutes les familles fabrique une fausse erosion : 63 % -> 44 % du call 0
-- au call 3, au lieu de 66 % -> 64 % sur les appels reellement traites.

with base as (
    select st.family_id, st.cohort_name,
        st.call_0_status, st.call1_status, st.call2_status, st.call3_status,
        st.was_engage_at_call0, st.was_engage_at_call1,
        st.was_engage_at_call2, st.was_engage_at_call3,
        st.date_stop_support,
        g.date_call0_end::date d0, g.date_call1_end::date d1,
        g.date_call2_end::date d2, g.date_call3_end::date d3
    from fact.super_table st
    inner join dim.child  c on c.child_id = st.child_id
    inner join dim.groups g on g.group_id = c.group_id
),
s as (
    select family_id, cohort_name, 0 cs, call_0_status stt, was_engage_at_call0 eng, date_stop_support dss, d0 fin from base
    union all select family_id, cohort_name, 1, call1_status, was_engage_at_call1, date_stop_support, d1 from base
    union all select family_id, cohort_name, 2, call2_status, was_engage_at_call2, date_stop_support, d2 from base
    union all select family_id, cohort_name, 3, call3_status, was_engage_at_call3, date_stop_support, d3 from base
),
a as (select family_id, cohort_name, cs, max(stt) stt, max(eng) eng,
             max(dss) dss, max(fin) fin from s group by 1,2,3)
select coalesce(eng,'(null)') as engagement,
       count(*) n,
       round(100.0*count(*)/sum(count(*)) over (),1)                              pct,
       round(100.0*count_if(nullif(trim(stt),'') is null)/count(*),1)             pct_sans_statut,
       round(100.0*count_if(dss is not null and dss <= fin)/count(*),1)           pct_arret_avant_session
from a
where cohort_name like '2026/%' and fin <= current_date
group by 1 order by 2 desc
