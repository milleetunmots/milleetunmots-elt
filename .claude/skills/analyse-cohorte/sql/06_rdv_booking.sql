-- 06 | Part de RDV (booking) et son effet sur le taux d'appels OK.
-- ATTENTION : le dispositif de RDV a demarre en FEVRIER 2026 et monte en charge tout
-- au long de l'annee (0 % de RDV au call 0 en janvier, 46 % en septembre). Toute
-- comparaison entre cohortes doit en tenir compte : une partie de la hausse du taux
-- d'OK des cohortes recentes vient du deploiement du RDV, pas d'une meilleure execution.
-- has_booking = RDV pris, TOUS STATUTS CONFONDUS (annulations incluses).
-- /!\ NE PAS LIRE ok_avec_rdv - ok_sans_rdv COMME L'EFFET DU DISPOSITIF.
-- Cet ecart de 25-30 pts est tres majoritairement de la SELECTION : les familles qui
-- prennent RDV sont deja celles qui repondent. L'effet reel, mesure hors biais par
-- 07_effet_rdv_adoption.sql, est d'environ +7 points, soit un quart de cet ecart.
-- Ne pas lire non plus l'evolution de ok_sans_rdv dans le temps (70 % en janvier,
-- 61 % en septembre) comme une degradation : a mesure que l'adoption monte, ce groupe
-- devient un residu de plus en plus negativement selectionne. Sa baisse est un artefact
-- de composition.
-- PARAM : remplacer '2026/%', retirer "where cs = 0" pour toutes sessions confondues.
with base as (
    select st.family_id, st.child_id, st.cohort_name,
        st.call_0_status, st.call1_status, st.call2_status, st.call3_status,
        st.is_call0_goals, st.is_call1_goals, st.is_call2_goals, st.is_call3_goals,
        st.has_booking_call0, st.has_booking_call1, st.has_booking_call2, st.has_booking_call3,
        st.has_booking_proposal_call0, st.has_booking_proposal_call1,
        st.has_booking_proposal_call2, st.has_booking_proposal_call3,
        g.date_call0_end::date d0, g.date_call1_end::date d1,
        g.date_call2_end::date d2, g.date_call3_end::date d3
    from fact.super_table st
    inner join dim.child  c on c.child_id = st.child_id
    inner join dim.groups g on g.group_id = c.group_id
),

sessions as (
    select family_id, cohort_name, 0 as call_session, call_0_status as call_status,
           is_call0_goals as is_goals, has_booking_call0 as rdv,
           has_booking_proposal_call0 as proposition, d0 as session_end from base
    union all select family_id, cohort_name, 1, call1_status, is_call1_goals,
           has_booking_call1, has_booking_proposal_call1, d1 from base
    union all select family_id, cohort_name, 2, call2_status, is_call2_goals,
           has_booking_call2, has_booking_proposal_call2, d2 from base
    union all select family_id, cohort_name, 3, call3_status, is_call3_goals,
           has_booking_call3, has_booking_proposal_call3, d3 from base
),

-- 1 ligne par famille x session (les fratries partagent le meme appel)
famille_session as (
    select family_id, cohort_name, call_session,
           max(call_status) as call_status,
           max(is_goals)    as is_goals,
           max(rdv)         as rdv,
           max(proposition) as proposition,
           max(session_end) as session_end
    from sessions group by 1, 2, 3
),

f as (
    select * from famille_session
    where session_end <= current_date
      and cohort_name like '2026/%'
      and nullif(trim(call_status), '') is not null   -- denominateur : appels avec statut
)

select
    left(cohort_name, 10)                                                  as date_cohorte,
    count(*)                                                               as nb_appels,
    round(100.0 * avg(proposition), 1)                                     as pct_rdv_propose,
    round(100.0 * avg(rdv), 1)                                             as pct_rdv_pris,
    round(100.0 * count_if(rdv = 1) / nullif(count_if(proposition = 1), 0), 1)
                                                                           as taux_transformation,
    round(100.0 * count_if(call_status='OK' and rdv=1) / nullif(count_if(rdv=1), 0), 1)
                                                                           as pct_ok_avec_rdv,
    round(100.0 * count_if(call_status='OK' and rdv=0) / nullif(count_if(rdv=0), 0), 1)
                                                                           as pct_ok_sans_rdv,
    round(100.0 * count_if(call_status='OK') / count(*), 1)                as pct_ok_global,
    -- Decomposition COMPTABLE : combien de points du chiffre de la cohorte viennent
    -- de familles ayant pris RDV. pct_ok_sans_rdv + part_comptable = pct_ok_global.
    -- Ce n'est PAS un gain attribuable au dispositif -- voir l'avertissement ci-dessus.
    round(avg(rdv) * (100.0 * count_if(call_status='OK' and rdv=1) / nullif(count_if(rdv=1), 0)
                    - 100.0 * count_if(call_status='OK' and rdv=0) / nullif(count_if(rdv=0), 0)), 1)
                                                                           as part_comptable_rdv
from f
where call_session = 0
group by 1
order by 1
