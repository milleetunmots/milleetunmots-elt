-- 10 | Decroche a la PREMIERE tentative, et effet du RDV dessus.
--
-- A ne pas confondre avec "appel 0 non abouti" (statut renseigne <> 'OK'), qui mesure
-- le resultat de la session entiere. Ici : la famille a-t-elle repondu des le premier
-- appel passe ?
--
-- MESURE DE REFERENCE : `nb_of_tries_callN` de super_table, saisi par l'accompagnante
-- dans le compte rendu d'appel. Valeurs : '1ere tentative', '2eme tentative',
-- '3eme tentative' (= 3 ou plus).
--
-- /!\ LE CHAMP N'EST RENSEIGNE QUE QUAND L'APPEL A ABOUTI : 99,5 % sur les statuts OK,
-- 96,2 % sur "Incomplet", mais 4,2 % sur les KO et 0 % sur "Numero errone". C'est une
-- rubrique du compte rendu, qui n'existe pas si personne n'a repondu. Deux lectures,
-- toutes les deux legitimes, a ne jamais melanger :
--   * BRUT   = '1ere tentative' / toutes les familles au statut renseigne. Les familles
--              jamais jointes comptent comme non decrochees -- ce qu'elles sont. C'est
--              le taux de decroche au sens courant.
--   * CONDITIONNEL = '1ere tentative' / familles effectivement jointes. Repond a
--              "quand on y arrive, combien d'essais faut-il ?".
--
-- Le champ est fiable malgre le piege PaperTrail de stg_1001mots_app__child_supports
-- (cf. la note projet) : 11 desaccords entre staging et source sur 34 202 familles.
--
-- CORROBORATION AIRCALL : le bloc (2) refait la mesure sur la duree des appels, sans
-- toucher au declaratif. `duration >= 120 s` sur la premiere tentative sortante donne
-- 35,3 % contre 36,1 % en brut -- deux sources independantes qui convergent.
--
-- Resultat (cohortes du dispositif, 2026/04/06 -> 2026/09/07, 2 550 familles) :
-- 36,1 % en brut, 51,3 % en conditionnel, 70,3 % de familles jointes.
-- RDV pris 58,9 / 66,7 / 88,4 ; propose sans RDV 24,0 / 42,1 / 56,9 ;
-- non propose 26,9 / 39,8 / 67,7.
--
-- NB : le decoupage propose / non propose suppose la regle `utm_source=1001mots` sur
-- has_booking_proposal_call0. Tant que super_table n'est pas reconstruite, la table
-- deployee porte l'ancienne regle et les deux groupes bougent d'environ 20 familles --
-- le total et le groupe RDV pris sont inchanges.
--
-- PARAM : la fenetre de cohortes ligne "between", et callN pour une autre session.

-- (1) Mesure de reference, par groupe RDV
with base as (
    select st.family_id, st.cohort_name, st.call_0_status, st.nb_of_tries_call0,
           st.has_booking_call0, st.has_booking_proposal_call0,
           g.date_call0_end::date as session_end
    from fact.super_table st
    inner join dim.child  c on c.child_id = st.child_id
    inner join dim.groups g on g.group_id = c.group_id
),

-- 1 ligne par famille (les fratries partagent le meme appel)
famille as (
    select family_id, cohort_name,
           max(call_0_status)              as call_status,
           max(nb_of_tries_call0)          as tries,
           max(has_booking_call0)          as rdv,
           max(has_booking_proposal_call0) as proposition,
           max(session_end)                as session_end
    from base group by 1, 2
),

f as (
    select * from famille
    where session_end <= current_date
      and cohort_name like '2026/%'
      and left(cohort_name, 10) between '2026/04/06' and '2026/09/07'   -- PARAM
      and nullif(trim(call_status), '') is not null                     -- regle 1
)

select
    case when rdv = 1 then '1 RDV pris'
         when proposition = 1 then '2 propose sans RDV'
         else '3 non propose' end                                          as groupe,
    count(*)                                                               as familles,
    round(100.0 * count_if(tries is not null) / count(*), 1)               as pct_jointes,
    round(100.0 * count_if(tries = '1ère tentative') / count(*), 1)        as decroche_1re_brut,
    round(100.0 * count_if(tries = '1ère tentative')
          / nullif(count_if(tries is not null), 0), 1)                     as decroche_1re_conditionnel,
    round(100.0 * count_if(tries = '2ème tentative')
          / nullif(count_if(tries is not null), 0), 1)                     as pct_2eme,
    round(100.0 * count_if(tries = '3ème tentative')
          / nullif(count_if(tries is not null), 0), 1)                     as pct_3eme_ou_plus
from f
group by 1
order by 1

;

-- (2) Corroboration Aircall, sur la duree du premier appel sortant.
-- Seuil `duration >= 120 s`, calibre sur le statut d'appel -- % de familles au statut OK
-- selon la duree max de la session : <30s 23,3 | 30-60s 14,6 | 60-90s 15,3 |
-- 90-120s 16,4 | 2-5min 46,6 | >5min 95,8. Rien sous 2 min ne produit d'appel OK.
-- `answered` est inutilisable (vrai dans 94,6 % des cas).
-- /!\ PLANCHER : un parent qui decroche et raccroche en 20 s est indiscernable d'une
-- sonnerie dans le repondeur. Le seuil a 30 s (92,3 %) ne mesure que le fait que la
-- ligne sonne -- ne jamais le publier comme un taux de decroche.
with base as (
    select st.family_id, st.cohort_name, st.call_0_status,
           st.has_booking_call0, st.has_booking_proposal_call0,
           g.date_call0_end::date as session_end
    from fact.super_table st
    inner join dim.child  c on c.child_id = st.child_id
    inner join dim.groups g on g.group_id = c.group_id
),

-- 1 ligne par famille (les fratries partagent le meme appel)
famille as (
    select family_id, cohort_name,
           max(call_0_status)              as call_status,
           max(has_booking_call0)          as rdv,
           max(has_booking_proposal_call0) as proposition,
           max(session_end)                as session_end
    from base group by 1, 2
),

-- tentatives Aircall de la session, classees dans l'ordre
tentatives as (
    select child_support_id as family_id, duration,
           row_number() over (partition by child_support_id order by started_at) as rang,
           count(*)       over (partition by child_support_id) as nb_tentatives,
           max(duration)  over (partition by child_support_id) as duree_max
    from staging.stg_1001mots_app__aircall_calls
    where call_session = 0 and direction = 'outbound'
),
premiere as (select * from tentatives where rang = 1),

f as (
    select fa.*, p.duration as duree_1re, p.nb_tentatives, p.duree_max
    from famille fa
    left join premiere p on p.family_id = fa.family_id
    where fa.session_end <= current_date
      and fa.cohort_name like '2026/%'
      and left(fa.cohort_name, 10) between '2026/04/06' and '2026/09/07'   -- PARAM
)

select
    case when rdv = 1 then '1 RDV pris'
         when proposition = 1 then '2 propose sans RDV'
         else '3 non propose' end                                          as groupe,
    count(*)                                                               as familles,
    count_if(duree_1re is null)                                            as sans_appel_aircall,
    round(avg(nb_tentatives), 2)                                           as tentatives_moy,
    -- ne mesure que le fait que la ligne sonne, NE PAS publier comme un decroche
    round(100.0 * count_if(duree_1re >= 30)  / nullif(count_if(duree_1re is not null), 0), 1) as sonne_30s,
    round(100.0 * count_if(duree_1re >= 120) / nullif(count_if(duree_1re is not null), 0), 1) as decroche_1re_tentative,
    round(100.0 * count_if(duree_1re >= 300) / nullif(count_if(duree_1re is not null), 0), 1) as conversation_longue_1re,
    round(100.0 * count_if(duree_max >= 120) / nullif(count_if(duree_1re is not null), 0), 1) as joint_au_final
from f
group by 1
order by 1
