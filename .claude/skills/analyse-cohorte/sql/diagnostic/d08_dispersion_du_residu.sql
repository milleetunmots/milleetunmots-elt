-- d08 | Quelle taille de residu est NORMALE ? A lancer avant d'expliquer un ecart.
-- (1) distribution des residus sur toutes les cellules 2025+2026
-- (2) part du bruit d'echantillonnage dans leur variance
-- Resultat : ecart-type de 6,3 points sur 128 cellules (249 appels en moyenne),
-- dont 27 % de la variance vient du seul bruit d'echantillonnage. Un residu de
-- -11,6 n'est donc qu'a 1,8 ecart-type, et sur 41 cellules le minimum attendu par
-- le SEUL HASARD est d'environ -13 points.
-- Autrement dit : la cohorte la plus basse d'une annee n'a pas forcement besoin
-- d'une explication. Toujours lancer cette requete avant de partir en diagnostic.

-- (1) Distribution par annee
with base as (
    select
        st.family_id, st.child_id, st.cohort_name, st.supporter_name,
        st.call_0_status, st.call1_status, st.call2_status, st.call3_status,
        st.is_call0_goals, st.is_call1_goals, st.is_call2_goals, st.is_call3_goals,
        g.date_call0_end::date as d0, g.date_call1_end::date as d1,
        g.date_call2_end::date as d2, g.date_call3_end::date as d3
    from fact.super_table as st
    inner join dim.child  as c on c.child_id = st.child_id
    inner join dim.groups as g on g.group_id = c.group_id
),

sessions as (
    select family_id, cohort_name, supporter_name, 0 as call_session,
           call_0_status as call_status, is_call0_goals as is_goals, d0 as session_end from base
    union all
    select family_id, cohort_name, supporter_name, 1, call1_status, is_call1_goals, d1 from base
    union all
    select family_id, cohort_name, supporter_name, 2, call2_status, is_call2_goals, d2 from base
    union all
    select family_id, cohort_name, supporter_name, 3, call3_status, is_call3_goals, d3 from base
),

-- 1 ligne par famille x session (les fratries partagent le même appel)
appel as (
    select
        family_id, cohort_name, supporter_name, call_session,
        max(session_end) as session_end,
        iff(max(call_status) = 'OK', 1, 0)                           as ok,
        iff(nullif(trim(max(call_status)),'') is not null, 1, 0)     as a_statut,
        iff(max(call_status) = 'OK' and max(is_goals) = 1, 1, 0)     as pm
    from sessions
    group by 1, 2, 3, 4
),

-- Univers complet (toutes années) : sert d'historique
appel_all as (
    select * from appel
    where session_end is not null
      and session_end <= current_date
      and a_statut = 1          -- <<< denominateur : appels ayant un statut
),

-- Cellules évaluées : cohorte x session, avec leur date de fin
cible as (
    select cohort_name, call_session, max(session_end) as fin
    from appel_all
    where left(cohort_name,4) in ('2025','2026')
    group by 1, 2
),

-- Volume de chaque accompagnante dans la cellule évaluée
volume as (
    select cohort_name, call_session, supporter_name,
           count(*) as n, count_if(ok = 1) as n_ok
    from appel_all
    where left(cohort_name,4) in ('2025','2026')
    group by 1, 2, 3
),

-- Historique 13 mois antérieurs de chaque accompagnante, MÊME session,
-- en excluant la cohorte évaluée (sinon la projection se prédit elle-même)
historique as (
    select
        v.cohort_name, v.call_session, v.supporter_name, v.n, v.n_ok,
        count(h.family_id)                   as n_hist,
        count_if(h.ok = 1)                   as n_hist_ok,
        avg(h.ok)                            as hist_ok,
        avg(iff(h.ok = 1, h.pm, null))       as hist_pm
    from volume as v
    join cible as c
      on c.cohort_name = v.cohort_name and c.call_session = v.call_session
    left join appel_all as h
      on  h.supporter_name = v.supporter_name
      and h.call_session   = v.call_session
      and h.cohort_name   <> v.cohort_name
      and h.session_end    <  c.fin
      and h.session_end   >= dateadd(month, -13, c.fin)
    group by 1, 2, 3, 4, 5
),

proj as (
    select cohort_name, call_session, sum(n) n,
           100.0*sum(iff(n_hist>0,n*hist_ok,0))/nullif(sum(iff(n_hist>0,n,0)),0) proj_ok
    from historique group by 1,2
),
reel as (
    select cohort_name, call_session, 100.0*avg(ok) reel_ok
    from appel_all where left(cohort_name,4) in ('2025','2026') group by 1,2
),
d as (
    select left(p.cohort_name,4) an, p.n, r.reel_ok - p.proj_ok ecart
    from proj p join reel r on r.cohort_name=p.cohort_name and r.call_session=p.call_session
    where p.n >= 100
)
select an, count(*) nb_cellules,
       round(avg(ecart),1) moyenne,
       round(stddev(ecart),1) ecart_type,
       round(min(ecart),1) mini,
       round(percentile_cont(0.05) within group (order by ecart),1) p5,
       round(percentile_cont(0.95) within group (order by ecart),1) p95,
       round(max(ecart),1) maxi
from d group by 1 order by 1

;

-- (2) Bruit d'echantillonnage contre variation reelle
with base as (
    select
        st.family_id, st.child_id, st.cohort_name, st.supporter_name,
        st.call_0_status, st.call1_status, st.call2_status, st.call3_status,
        st.is_call0_goals, st.is_call1_goals, st.is_call2_goals, st.is_call3_goals,
        g.date_call0_end::date as d0, g.date_call1_end::date as d1,
        g.date_call2_end::date as d2, g.date_call3_end::date as d3
    from fact.super_table as st
    inner join dim.child  as c on c.child_id = st.child_id
    inner join dim.groups as g on g.group_id = c.group_id
),

sessions as (
    select family_id, cohort_name, supporter_name, 0 as call_session,
           call_0_status as call_status, is_call0_goals as is_goals, d0 as session_end from base
    union all
    select family_id, cohort_name, supporter_name, 1, call1_status, is_call1_goals, d1 from base
    union all
    select family_id, cohort_name, supporter_name, 2, call2_status, is_call2_goals, d2 from base
    union all
    select family_id, cohort_name, supporter_name, 3, call3_status, is_call3_goals, d3 from base
),

-- 1 ligne par famille x session (les fratries partagent le même appel)
appel as (
    select
        family_id, cohort_name, supporter_name, call_session,
        max(session_end) as session_end,
        iff(max(call_status) = 'OK', 1, 0)                           as ok,
        iff(nullif(trim(max(call_status)),'') is not null, 1, 0)     as a_statut,
        iff(max(call_status) = 'OK' and max(is_goals) = 1, 1, 0)     as pm
    from sessions
    group by 1, 2, 3, 4
),

-- Univers complet (toutes années) : sert d'historique
appel_all as (
    select * from appel
    where session_end is not null
      and session_end <= current_date
      and a_statut = 1          -- <<< denominateur : appels ayant un statut
),

-- Cellules évaluées : cohorte x session, avec leur date de fin
cible as (
    select cohort_name, call_session, max(session_end) as fin
    from appel_all
    where left(cohort_name,4) in ('2025','2026')
    group by 1, 2
),

-- Volume de chaque accompagnante dans la cellule évaluée
volume as (
    select cohort_name, call_session, supporter_name,
           count(*) as n, count_if(ok = 1) as n_ok
    from appel_all
    where left(cohort_name,4) in ('2025','2026')
    group by 1, 2, 3
),

-- Historique 13 mois antérieurs de chaque accompagnante, MÊME session,
-- en excluant la cohorte évaluée (sinon la projection se prédit elle-même)
historique as (
    select
        v.cohort_name, v.call_session, v.supporter_name, v.n, v.n_ok,
        count(h.family_id)                   as n_hist,
        count_if(h.ok = 1)                   as n_hist_ok,
        avg(h.ok)                            as hist_ok,
        avg(iff(h.ok = 1, h.pm, null))       as hist_pm
    from volume as v
    join cible as c
      on c.cohort_name = v.cohort_name and c.call_session = v.call_session
    left join appel_all as h
      on  h.supporter_name = v.supporter_name
      and h.call_session   = v.call_session
      and h.cohort_name   <> v.cohort_name
      and h.session_end    <  c.fin
      and h.session_end   >= dateadd(month, -13, c.fin)
    group by 1, 2, 3, 4, 5
),

proj as (
    select cohort_name, call_session, sum(n) n,
           100.0*sum(iff(n_hist>0,n*hist_ok,0))/nullif(sum(iff(n_hist>0,n,0)),0) proj_ok
    from historique group by 1,2
),
reel as (
    select cohort_name, call_session, 100.0*avg(ok) reel_ok
    from appel_all where left(cohort_name,4) in ('2025','2026') group by 1,2
),
d as (
    select left(p.cohort_name,4) an, p.n, r.reel_ok - p.proj_ok ecart
    from proj p join reel r on r.cohort_name=p.cohort_name and r.call_session=p.call_session
    where p.n >= 100
)
select count(*) nb_cellules,
       round(avg(n),0) taille_moy,
       round(stddev(ecart),2) sd_totale,
       round(sqrt(avg(reel_ok*(100-reel_ok)/n)),2) sd_bruit_attendue,
       round(sqrt(greatest(variance(ecart) - avg(reel_ok*(100-reel_ok)/n),0)),2) sd_vraie,
       round(100.0*avg(reel_ok*(100-reel_ok)/n)/variance(ecart),0) pct_variance_bruit
from (select p.n, r.reel_ok, r.reel_ok - p.proj_ok ecart
      from proj p join reel r on r.cohort_name=p.cohort_name and r.call_session=p.call_session
      where p.n >= 100)
