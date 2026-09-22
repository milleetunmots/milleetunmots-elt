-- 05 | Projection integrant le CANAL DE RECRUTEMENT (caf / pmi / local_partner / bao).
-- Sert a savoir quelle part d'un ecart vient du mix de recrutement de la cohorte
-- plutot que de la performance des accompagnantes.
-- Historique par accompagnante x session x canal sur 13 mois glissants, repli sur
-- l'historique tous canaux quand le canal a moins de 20 appels d'historique
-- (colonne pct_canal_dispo = part du volume ayant un historique canal propre).
-- L'ecart_residuel est ce qui reste APRES neutralisation du mix de recrutement.
-- PARAM : remplacer '2026/%' (4 occurrences).
-- Projeté vs réalisé par cohorte x session
-- Projection = performance des accompagnantes de la cellule sur les 13 mois
-- PRÉCÉDANT la fin de la session, même session, cohorte évaluée exclue.
with base as (
    select
        st.family_id, st.child_id, st.cohort_name, st.supporter_name, st.source_channel as canal,
        st.call_0_status, st.call1_status, st.call2_status, st.call3_status,
        st.is_call0_goals, st.is_call1_goals, st.is_call2_goals, st.is_call3_goals,
        g.date_call0_end::date as d0, g.date_call1_end::date as d1,
        g.date_call2_end::date as d2, g.date_call3_end::date as d3
    from fact.super_table as st
    inner join dim.child  as c on c.child_id = st.child_id
    inner join dim.groups as g on g.group_id = c.group_id
),

sessions as (
    select family_id, cohort_name, supporter_name, canal, 0 as call_session,
           call_0_status as call_status, is_call0_goals as is_goals, d0 as session_end from base
    union all
    select family_id, cohort_name, supporter_name, canal, 1, call1_status, is_call1_goals, d1 from base
    union all
    select family_id, cohort_name, supporter_name, canal, 2, call2_status, is_call2_goals, d2 from base
    union all
    select family_id, cohort_name, supporter_name, canal, 3, call3_status, is_call3_goals, d3 from base
),

-- 1 ligne par famille x session (les fratries partagent le même appel)
appel as (
    select
        family_id, cohort_name, supporter_name, canal, call_session,
        max(session_end) as session_end,
        iff(max(call_status) = 'OK', 1, 0)                           as ok,
        iff(nullif(trim(max(call_status)),'') is not null, 1, 0)     as a_statut,
        iff(max(call_status) = 'OK' and max(is_goals) = 1, 1, 0)     as pm
    from sessions
    group by 1, 2, 3, 4, 5
),

-- Univers complet (toutes années) : sert d'historique
appel_all as (
    select * from appel
    where session_end is not null
      and session_end <= current_date
      and a_statut = 1          -- <<< denominateur : appels ayant un statut
),

cible as (
    select cohort_name, call_session, max(session_end) as fin
    from appel_all where cohort_name like '2026/%' group by 1,2
),
volume as (
    select cohort_name, call_session, supporter_name, canal,
           count(*) as n, count_if(ok=1) as n_ok
    from appel_all where cohort_name like '2026/%' group by 1,2,3,4
),
-- historique 13 mois : meme accompagnante, meme session, MEME CANAL
hist_canal as (
    select v.cohort_name, v.call_session, v.supporter_name, v.canal, v.n,
           count(h.family_id) as n_hist, avg(h.ok) as hist_ok
    from volume v
    join cible c on c.cohort_name=v.cohort_name and c.call_session=v.call_session
    left join appel_all h
      on h.supporter_name=v.supporter_name and h.call_session=v.call_session
     and equal_null(h.canal, v.canal)
     and h.cohort_name<>v.cohort_name
     and h.session_end < c.fin and h.session_end >= dateadd(month,-13,c.fin)
    group by 1,2,3,4,5
),
-- historique 13 mois : meme accompagnante, meme session, TOUS CANAUX (repli)
hist_tous as (
    select v.cohort_name, v.call_session, v.supporter_name, v.canal, v.n,
           count(h.family_id) as n_hist2, avg(h.ok) as hist_ok2
    from volume v
    join cible c on c.cohort_name=v.cohort_name and c.call_session=v.call_session
    left join appel_all h
      on h.supporter_name=v.supporter_name and h.call_session=v.call_session
     and h.cohort_name<>v.cohort_name
     and h.session_end < c.fin and h.session_end >= dateadd(month,-13,c.fin)
    group by 1,2,3,4,5
),
comb as (
    select a.cohort_name, a.call_session, a.n,
           iff(a.n_hist >= 20, a.hist_ok, b.hist_ok2) as proj_unit,
           iff(a.n_hist >= 20, 1, 0) as canal_dispo
    from hist_canal a
    join hist_tous b on b.cohort_name=a.cohort_name and b.call_session=a.call_session
                    and b.supporter_name=a.supporter_name and equal_null(b.canal,a.canal)
),
proj2 as (
    select cohort_name, call_session, sum(n) as n,
           100.0*sum(n*proj_unit)/nullif(sum(iff(proj_unit is not null, n, 0)),0) as proj_ok_canal,
           round(100.0*sum(iff(canal_dispo=1,n,0))/sum(n),0) as pct_canal_dispo
    from comb group by 1,2
),
reel as (
    select cohort_name, call_session, 100.0*avg(ok) as reel_ok
    from appel_all where cohort_name like '2026/%' group by 1,2
)
select left(p.cohort_name,10) as coh, p.call_session as s, p.n as appels,
       round(p.proj_ok_canal,1) as proj_avec_canal,
       round(r.reel_ok,1) as reel,
       round(r.reel_ok-p.proj_ok_canal,1) as ecart_residuel,
       p.pct_canal_dispo
from proj2 p join reel r on r.cohort_name=p.cohort_name and r.call_session=p.call_session
-- PARAM : restreindre ici aux cellules a diagnostiquer, ou retirer le filtre
-- where left(p.cohort_name,10) in ('2026/04/06','2026/04/20')
order by 1,2
