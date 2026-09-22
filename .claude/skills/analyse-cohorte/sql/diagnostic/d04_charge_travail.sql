-- d04 | Charge de travail : au niveau de la fenetre d'appel, et au niveau individuel.
-- (1) Appels a passer sur la fenetre d'une cellule, TOUTES COHORTES CONFONDUES,
--     rapportes au nombre d'accompagnantes. Une cellule peut etre normale isolement
--     et tomber au milieu d'un pic.
-- (2) Charge de chaque accompagnante rapportee a SON habitude sur 13 mois.
-- Resultat (2026) : repere de 50 a 55 appels par accompagnante et par fenetre de
-- 13 jours. Les cohortes d'avril etaient a ~50, dans la fourchette BASSE. Et la
-- charge relative individuelle ne predit rien : l'ecart moyen va de 0,0 a +1,4 pt
-- entre une charge allegee (<0,7x) et une surcharge (>1,4x), sans progression.
-- Hypothese ecartee sur les deux niveaux.

-- (1) Charge de la fenetre
with base as (
    select st.family_id, st.cohort_name, st.supporter_name,
        g.date_call0_start::date s0, g.date_call0_end::date e0,
        g.date_call1_start::date s1, g.date_call1_end::date e1,
        g.date_call2_start::date s2, g.date_call2_end::date e2,
        g.date_call3_start::date s3, g.date_call3_end::date e3
    from fact.super_table st
    inner join dim.child c on c.child_id=st.child_id
    inner join dim.groups g on g.group_id=c.group_id
),
sess as (
    select family_id, cohort_name, supporter_name, 0 cs, s0 deb, e0 fin from base
    union all select family_id, cohort_name, supporter_name, 1, s1, e1 from base
    union all select family_id, cohort_name, supporter_name, 2, s2, e2 from base
    union all select family_id, cohort_name, supporter_name, 3, s3, e3 from base
),
a as (select distinct family_id, cohort_name, supporter_name, cs, deb, fin from sess where fin is not null),
-- charge totale de chaque fenetre : tous les appels a passer dans la meme periode
charge as (
    select x.cohort_name, x.cs,
           max(x.deb) as deb, max(x.fin) as fin,
           count(distinct x.family_id) as appels_cellule,
           count(distinct x.supporter_name) as nb_acc,
           count(distinct x.family_id)/count(distinct x.supporter_name) as par_acc_cellule
    from a x
    where x.cohort_name like '2026/%'
    group by 1,2
),
acc_cellule as (
    select distinct cohort_name, cs, supporter_name
    from a where cohort_name like '2026/%'
),
charge_totale as (
    select c.cohort_name, c.cs, c.appels_cellule, c.nb_acc, c.par_acc_cellule,
           count(distinct y.family_id || '_' || y.cs) as appels_fenetre
    from charge c
    join acc_cellule ac on ac.cohort_name = c.cohort_name and ac.cs = c.cs
    join a y on y.supporter_name = ac.supporter_name
            and y.deb <= c.fin and y.fin >= c.deb
    group by 1,2,3,4,5
)
select left(cohort_name,10) coh, cs s, appels_cellule, nb_acc,
       round(par_acc_cellule,1) as par_acc,
       appels_fenetre,
       round(appels_fenetre/nb_acc,0) as charge_par_acc
from charge_totale
where left(cohort_name,10) between '2026/01/12' and '2026/06/01'
order by 1,2

;

-- (2) Charge individuelle relative a l'habitude
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
    where cohort_name like '2026/%'
    group by 1, 2
),

-- Volume de chaque accompagnante dans la cellule évaluée
volume as (
    select cohort_name, call_session, supporter_name,
           count(*) as n, count_if(ok = 1) as n_ok
    from appel_all
    where cohort_name like '2026/%'
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

-- Charge habituelle de chaque accompagnante : nb d'appels median par cellule
-- sur les 13 mois precedents (meme session)
charge_habituelle as (
    select v.cohort_name, v.call_session, v.supporter_name, v.n as n_cellule,
           avg(x.n_autre) as n_habituel
    from volume v
    join cible c on c.cohort_name=v.cohort_name and c.call_session=v.call_session
    join (
        select supporter_name, cohort_name, call_session, max(session_end) fin,
               count(*) as n_autre
        from appel_all group by 1,2,3
    ) x
      on x.supporter_name = v.supporter_name
     and x.call_session   = v.call_session
     and x.cohort_name   <> v.cohort_name
     and x.fin <  c.fin and x.fin >= dateadd(month,-13,c.fin)
    group by 1,2,3,4
),
reel_acc as (
    select cohort_name, call_session, supporter_name, 100.0*avg(ok) reel_ok
    from appel_all where cohort_name like '2026/%' group by 1,2,3
),
d as (
    select h.cohort_name, h.call_session, h.supporter_name, h.n,
           ch.n_habituel,
           h.n / nullif(ch.n_habituel,0) as ratio_charge,
           r.reel_ok - 100.0*h.hist_ok   as ecart
    from historique h
    join charge_habituelle ch on ch.cohort_name=h.cohort_name and ch.call_session=h.call_session
                             and ch.supporter_name=h.supporter_name
    join reel_acc r on r.cohort_name=h.cohort_name and r.call_session=h.call_session
                   and r.supporter_name=h.supporter_name
    where h.n_hist >= 20 and h.n >= 8
)
select case when ratio_charge < 0.7 then 'a. <0,7 (charge allegee)'
            when ratio_charge < 0.9 then 'b. 0,7-0,9'
            when ratio_charge < 1.1 then 'c. 0,9-1,1 (normale)'
            when ratio_charge < 1.4 then 'd. 1,1-1,4'
            else 'e. >1,4 (surcharge)' end as tranche_charge,
       count(*) nb_cellules, sum(n) appels,
       round(sum(n*ecart)/sum(n),1) ecart_moyen
from d group by 1 order by 1
