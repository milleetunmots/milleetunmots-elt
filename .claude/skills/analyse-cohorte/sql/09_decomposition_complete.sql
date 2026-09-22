-- 09 | DECOMPOSITION COMPLETE des deux metriques, couche par couche.
--
--   reel = barre du vivier + effet pool + effet canal + residu
--
-- Couche 1  barre du vivier  : toutes les accompagnantes actives, ponderees capacite
-- Couche 2  effet pool       : qui a ete mobilise, et en quels volumes
-- Couche 3  effet canal      : mix de recrutement (caf / pmi / local_partner)
-- Couche 4  residu           : ce qui reste
--
-- La chaine est additive et exacte a l'arrondi pres.
--
-- /!\ LE DISPOSITIF RDV N'EST PAS UNE COUCHE. L'ajouter produit des chiffres faux :
-- la projection du pool s'appuie sur l'historique 13 mois, qui pour les cohortes
-- recentes CONTIENT DEJA leurs cohortes RDV -- on le retirerait deux fois. La colonne
-- pct_adoptantes permet d'en estimer une borne haute (part x +7 points, cf. 07).
--
-- Resultat (2026, call 0) : l'effet pool pese DEUX FOIS PLUS sur les missions
-- (7,1 pts d'amplitude) que sur les appels OK (5,5), coherent avec l'ecart individuel
-- entre accompagnantes (17 pts d'ecart-type sur les missions contre 7 sur les OK).
-- Le canal, lui, ne joue presque pas sur les missions (-1,7 a +1,0) alors qu'il pese
-- jusqu'a 4 points sur les appels OK : il determine si on joint la famille, pas ce
-- qu'on fait une fois au telephone.
--
-- /!\ Avant d'interpreter un residu, lancer diagnostic/d08_dispersion_du_residu.sql :
-- la dispersion normale est de 6,3 points.
-- PARAM : '2026/%' et call_session = 0.

with base as (
    select
        st.family_id, st.child_id, st.cohort_name, st.supporter_name, st.source_channel as canal,
        st.call_0_status, st.call1_status, st.call2_status, st.call3_status,
        st.is_call0_goals, st.is_call1_goals, st.is_call2_goals, st.is_call3_goals,
        st.has_booking_proposal_call0 pr0, st.has_booking_proposal_call1 pr1,
        st.has_booking_proposal_call2 pr2, st.has_booking_proposal_call3 pr3,
        g.date_call0_end::date as d0, g.date_call1_end::date as d1,
        g.date_call2_end::date as d2, g.date_call3_end::date as d3
    from fact.super_table as st
    inner join dim.child  as c on c.child_id = st.child_id
    inner join dim.groups as g on g.group_id = c.group_id
),

sessions as (
    select family_id, cohort_name, supporter_name, canal, 0 as call_session,
           call_0_status as call_status, is_call0_goals as is_goals, pr0 as prop, d0 as session_end from base
    union all
    select family_id, cohort_name, supporter_name, canal, 1, call1_status, is_call1_goals, pr1, d1 from base
    union all
    select family_id, cohort_name, supporter_name, canal, 2, call2_status, is_call2_goals, pr2, d2 from base
    union all
    select family_id, cohort_name, supporter_name, canal, 3, call3_status, is_call3_goals, pr3, d3 from base
),

-- 1 ligne par famille x session (les fratries partagent le même appel)
appel as (
    select
        family_id, cohort_name, supporter_name, canal, call_session,
        max(session_end) as session_end,
        iff(max(call_status) = 'OK', 1, 0)                           as ok,
        iff(nullif(trim(max(call_status)),'') is not null, 1, 0)     as a_statut,
        iff(max(call_status) = 'OK' and max(is_goals) = 1, 1, 0)     as pm,
        max(prop)                                                    as prop
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

-- ---------- COUCHE 1 : barre du vivier (toutes accompagnantes actives) ----------
tous_supporters as (select distinct supporter_name from appel_all),
vivier as (
    select c.cohort_name, c.call_session, s.supporter_name,
           count(h.family_id) n_hist, count_if(h.ok=1) n_hist_ok,
           avg(h.ok) hist_ok, avg(iff(h.ok=1,h.pm,null)) hist_pm
    from cible c cross join tous_supporters s
    left join appel_all h
      on h.supporter_name=s.supporter_name and h.call_session=c.call_session
     and h.cohort_name<>c.cohort_name
     and h.session_end < c.fin and h.session_end >= dateadd(month,-13,c.fin)
    group by 1,2,3 having count(h.family_id) >= 20
),
barre as (
    select cohort_name, call_session, count(*) taille_vivier,
           100.0*sum(n_hist*hist_ok)/sum(n_hist)                              ref_ok,
           100.0*sum(iff(hist_pm is not null, n_hist_ok*hist_pm,0))
                /nullif(sum(iff(hist_pm is not null, n_hist_ok,0)),0)         ref_pm
    from vivier group by 1,2
),

-- ---------- COUCHE 2 : pool effectivement mobilise ----------
volume as (
    select cohort_name, call_session, supporter_name, canal,
           count(*) n, count_if(ok=1) n_ok
    from appel_all where cohort_name like '2026/%' group by 1,2,3,4
),
vol_acc as (
    select cohort_name, call_session, supporter_name, sum(n) n, sum(n_ok) n_ok
    from volume group by 1,2,3
),
pool as (
    select v.cohort_name, v.call_session, count(*) taille_pool, sum(v.n) appels,
           100.0*sum(v.n*b.hist_ok)/nullif(sum(iff(b.hist_ok is not null,v.n,0)),0)  proj_ok,
           100.0*sum(iff(b.hist_pm is not null, v.n_ok*b.hist_pm,0))
                /nullif(sum(iff(b.hist_pm is not null, v.n_ok,0)),0)                 proj_pm
    from vol_acc v
    join vivier b on b.cohort_name=v.cohort_name and b.call_session=v.call_session
                 and b.supporter_name=v.supporter_name
    group by 1,2
),

-- ---------- COUCHE 3 : canal de recrutement ----------
hist_canal as (
    select v.cohort_name, v.call_session, v.supporter_name, v.canal, v.n, v.n_ok,
           count(h.family_id) nh, count_if(h.ok=1) nhok,
           avg(h.ok) hok, avg(iff(h.ok=1,h.pm,null)) hpm
    from volume v
    join cible c on c.cohort_name=v.cohort_name and c.call_session=v.call_session
    left join appel_all h
      on h.supporter_name=v.supporter_name and h.call_session=v.call_session
     and equal_null(h.canal,v.canal) and h.cohort_name<>v.cohort_name
     and h.session_end < c.fin and h.session_end >= dateadd(month,-13,c.fin)
    group by 1,2,3,4,5,6
),
comb as (
    select hc.cohort_name, hc.call_session, hc.n, hc.n_ok,
           iff(hc.nh   >= 20, hc.hok, vi.hist_ok) u_ok,
           iff(hc.nhok >= 15, hc.hpm, vi.hist_pm) u_pm
    from hist_canal hc
    join vivier vi on vi.cohort_name=hc.cohort_name and vi.call_session=hc.call_session
                  and vi.supporter_name=hc.supporter_name
),
pool_canal as (
    select cohort_name, call_session,
           100.0*sum(iff(u_ok is not null, n*u_ok,0))/nullif(sum(iff(u_ok is not null,n,0)),0)      proj_c_ok,
           100.0*sum(iff(u_pm is not null, n_ok*u_pm,0))/nullif(sum(iff(u_pm is not null,n_ok,0)),0) proj_c_pm
    from comb group by 1,2
),

-- ---------- COUCHE 4 : adoption du dispositif RDV ----------
adoption as (
    select cohort_name, call_session,
           sum(iff(tp >= 0.5, n, 0)) / sum(n) as part_adoptantes
    from (select cohort_name, call_session, supporter_name, count(*) n, avg(prop) tp
          from appel_all where cohort_name like '2026/%' group by 1,2,3)
    group by 1,2
),

reel as (
    select cohort_name, call_session,
           100.0*avg(ok) reel_ok, 100.0*avg(iff(ok=1,pm,null)) reel_pm
    from appel_all where cohort_name like '2026/%' group by 1,2
)

select left(p.cohort_name,10) coh, p.taille_pool, b.taille_vivier, p.appels,
       round(b.ref_ok,1) ref_ok, round(p.proj_ok,1) proj_ok,
       round(pc.proj_c_ok,1) proj_c_ok, round(r.reel_ok,1) reel_ok,
       round(100.0*a.part_adoptantes,0) pct_adopt,
       round(b.ref_pm,1) ref_pm, round(p.proj_pm,1) proj_pm,
       round(pc.proj_c_pm,1) proj_c_pm, round(r.reel_pm,1) reel_pm
from pool p
join barre b      on b.cohort_name=p.cohort_name and b.call_session=p.call_session
join pool_canal pc on pc.cohort_name=p.cohort_name and pc.call_session=p.call_session
join adoption a   on a.cohort_name=p.cohort_name and a.call_session=p.call_session
join reel r       on r.cohort_name=p.cohort_name and r.call_session=p.call_session
where p.call_session = 0
order by 1
