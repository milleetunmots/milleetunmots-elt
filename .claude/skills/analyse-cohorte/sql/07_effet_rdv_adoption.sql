-- 07 | Effet REEL du dispositif de RDV, hors biais de selection.
--
-- Le probleme : comparer les familles avec RDV a celles sans RDV surestime enormement
-- l'effet (25-30 pts d'ecart apparent), parce que les familles qui prennent RDV sont
-- deja celles qui repondent. Et comparer le taux "hors RDV" dans le temps ne marche pas
-- non plus : a mesure que l'adoption monte, ce groupe devient un residu de plus en plus
-- negativement selectionne.
--
-- La solution : la PROPOSITION de RDV est une pratique d'accompagnante, en tout ou rien
-- (dans chaque cohorte la plupart sont a 0 % ou a ~100 %, presque personne entre les
-- deux). On compare donc, DANS UNE MEME COHORTE, la TOTALITE des familles des
-- accompagnantes adoptantes a celle des non adoptantes -- aucune selection au niveau
-- famille. Et on mesure chaque groupe par son ecart a SA PROPRE projection 13 mois,
-- ce qui neutralise le fait que les adoptantes precoces pourraient etre les meilleures.
--
-- Resultat sur 2026 (call 0) : +7 points en moyenne en faveur des adoptantes, sur
-- 4 cohortes comparables sur 5. Soit environ un quart de l'ecart brut.
--
-- Limites : effectifs modestes par groupe (60 a 360 appels), incertitude de 5 a 8 pts
-- par cohorte -- c'est la coherence entre cohortes qui porte le resultat. Et le test
-- suppose que les familles sont reparties de facon comparable entre adoptantes et non
-- adoptantes au sein d'une cohorte ; un biais residuel subsiste si l'affectation suit
-- le departement ou la langue. Non verifie.
--
-- PARAM : remplacer '2026/%' ; call_session = 0 ; seuils 8 appels / cellule et
-- 60 appels / groupe pour l'affichage par cohorte.
with base as (
    select
        st.family_id, st.child_id, st.cohort_name, st.supporter_name,
        st.call_0_status, st.call1_status, st.call2_status, st.call3_status,
        st.is_call0_goals, st.is_call1_goals, st.is_call2_goals, st.is_call3_goals,
        st.has_booking_proposal_call0 as prop0, st.has_booking_proposal_call1 as prop1,
        st.has_booking_proposal_call2 as prop2, st.has_booking_proposal_call3 as prop3,
        g.date_call0_end::date as d0, g.date_call1_end::date as d1,
        g.date_call2_end::date as d2, g.date_call3_end::date as d3
    from fact.super_table as st
    inner join dim.child  as c on c.child_id = st.child_id
    inner join dim.groups as g on g.group_id = c.group_id
),

sessions as (
    select family_id, cohort_name, supporter_name, 0 as call_session,
           call_0_status as call_status, is_call0_goals as is_goals, prop0 as prop, d0 as session_end from base
    union all
    select family_id, cohort_name, supporter_name, 1, call1_status, is_call1_goals, prop1, d1 from base
    union all
    select family_id, cohort_name, supporter_name, 2, call2_status, is_call2_goals, prop2, d2 from base
    union all
    select family_id, cohort_name, supporter_name, 3, call3_status, is_call3_goals, prop3, d3 from base
),

-- 1 ligne par famille x session (les fratries partagent le même appel)
appel as (
    select
        family_id, cohort_name, supporter_name, call_session,
        max(session_end) as session_end,
        iff(max(call_status) = 'OK', 1, 0)                           as ok,
        iff(nullif(trim(max(call_status)),'') is not null, 1, 0)     as a_statut,
        iff(max(call_status) = 'OK' and max(is_goals) = 1, 1, 0)     as pm,
        max(prop)                                                    as prop
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

-- Adoption de la proposition de RDV, au niveau accompagnante x cohorte x session
adoption as (
    select cohort_name, call_session, supporter_name,
           count(*) as n,
           avg(prop) as taux_proposition,
           case when avg(prop) =  0   then 'NON ADOPTANTE'
                when avg(prop) >= 0.5 then 'ADOPTANTE' end as groupe
    from appel_all
    where cohort_name like '2026/%'
    group by 1, 2, 3
    having count(*) >= 8            -- volume minimum pour qualifier une pratique
),

reel_acc as (
    select cohort_name, call_session, supporter_name,
           count(*) as n_reel, 100.0 * avg(ok) as reel_ok
    from appel_all
    where cohort_name like '2026/%'
    group by 1, 2, 3
)

select
    left(h.cohort_name, 10)                                   as date_cohorte,
    ad.groupe,
    sum(h.n)                                                  as nb_appels,
    round(sum(h.n * 100.0 * h.hist_ok) / sum(h.n), 1)         as projete,
    round(sum(r.n_reel * r.reel_ok) / sum(r.n_reel), 1)       as reel,
    round(sum(r.n_reel * r.reel_ok) / sum(r.n_reel)
        - sum(h.n * 100.0 * h.hist_ok) / sum(h.n), 1)         as ecart
from historique as h
join adoption as ad
  on ad.cohort_name = h.cohort_name and ad.call_session = h.call_session
 and ad.supporter_name = h.supporter_name
join reel_acc as r
  on r.cohort_name = h.cohort_name and r.call_session = h.call_session
 and r.supporter_name = h.supporter_name
where ad.groupe is not null
  and h.n_hist >= 20
  and h.call_session = 0
group by 1, 2
having sum(h.n) >= 60
order by 1, 2
-- Pour l'estimation poolee toutes cohortes : retirer le "left(h.cohort_name,10)" du
-- select et du group by. ATTENTION : le pool est biaise par la composition (les non
-- adoptantes sont sur-representees dans les bonnes cohortes de debut d'annee). La
-- comparaison PAR COHORTE ci-dessus est la bonne.
