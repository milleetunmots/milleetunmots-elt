-- d03 | Mix de recrutement par cohorte, et performance de chaque canal.
-- Deux requetes : la composition, puis les taux par canal mesures HORS des
-- cohortes evaluees (sinon circulaire).
-- Resultat (2026) : la part CAF monte de 16 % en janvier a 99 % le 20 avril puis
-- redescend. Correlation part CAF / ecart au projete : r = -0,87 sur 13 cohortes.
-- Taux d'OK au call 0 : caf 62,2 / pmi 72,1 / local_partner 71,2.
-- Quantifier avec ../05_projection_avec_canal.sql.

-- (1) Composition par cohorte
select left(cohort_name,10) coh, count(*) n,
       round(100.0*count_if(source_channel='caf')/count(*),0)           pct_caf,
       round(100.0*count_if(source_channel='pmi')/count(*),0)           pct_pmi,
       round(100.0*count_if(source_channel='local_partner')/count(*),0) pct_partenaire
from fact.super_table
where cohort_name like '2026/%'
group by 1 order by 1;

-- (2) Performance par canal, hors cohortes evaluees
with base as (
    select st.family_id, st.cohort_name, st.source_channel,
        st.call_0_status, st.call1_status, st.call2_status, st.call3_status,
        st.is_call0_goals, st.is_call1_goals, st.is_call2_goals, st.is_call3_goals,
        g.date_call0_end::date d0, g.date_call1_end::date d1,
        g.date_call2_end::date d2, g.date_call3_end::date d3
    from fact.super_table st
    inner join dim.child  c on c.child_id = st.child_id
    inner join dim.groups g on g.group_id = c.group_id
),
s as (
    select family_id, cohort_name, source_channel, 0 cs, call_0_status stt, is_call0_goals gl, d0 fin from base
    union all select family_id, cohort_name, source_channel, 1, call1_status, is_call1_goals, d1 from base
    union all select family_id, cohort_name, source_channel, 2, call2_status, is_call2_goals, d2 from base
    union all select family_id, cohort_name, source_channel, 3, call3_status, is_call3_goals, d3 from base
),
a as (select family_id, cohort_name, max(source_channel) canal, cs,
             max(stt) stt, max(gl) gl, max(fin) fin from s group by 1,2,4)
select coalesce(canal,'(null)') canal, cs as session_appel, count(*) n,
       round(100.0*count_if(stt='OK')/count(*),1) pct_ok,
       round(100.0*count_if(stt='OK' and gl=1)/nullif(count_if(stt='OK'),0),1) pct_pm
from a
where fin <= current_date and cohort_name like '2026/%'
  and nullif(trim(stt),'') is not null
  and left(cohort_name,10) not in ('2026/04/06','2026/04/20')   -- PARAM : cohortes evaluees
group by 1,2 having count(*) >= 50
order by 2,1
