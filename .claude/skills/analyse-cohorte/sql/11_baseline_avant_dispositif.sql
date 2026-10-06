-- 11 | Ligne de base AVANT le dispositif RDV, et comparaison au regime etabli.
--
-- Pourquoi ce fichier : le contraste "propose / non propose" a la meme periode ne mesure
-- que de la selection. Le bon comparateur d'un groupe propose est la situation d'AVANT,
-- pas ses contemporains non proposes.
--
-- FAIT ETABLI : la proposition est PROACTIVE. Le SMS part en moyenne 3 jours avant
-- l'ouverture de la fenetre d'appel, et 99 % des propositions precedent le moindre appel
-- (0,03 tentative en amont en moyenne, 98 % des premiers appels plus de 3 jours apres le
-- SMS). L'hypothese "le lien est envoye apres un premier echec de contact" a ete
-- construite puis INVALIDEE sur les horodatages Aircall -- ne pas la refaire.
--
-- Resultat au 2026-09-29, call 0, familles au statut renseigne :
--   A. avant dispositif  2025/01-2026/01  6 316 fam  0,2 % propose  70,9 % jointes
--                        decroche brut 30,4  cond 42,9  2,75 tentatives  65,9 % OK
--   B. demarrage         2026/02-2026/04  1 615 fam  18,0 %          67,0 %
--                        26,9  40,2  2,82  61,0
--   C. regime etabli     2026/05-2026/09  2 028 fam  74,7 %          73,0 %
--                        40,1  55,0  2,41  67,4
--   C1. RDV pris            701 fam  89,3 % jointes  61,2  68,5  2,02  86,6
--   C2. propose sans RDV    813 fam  57,6 %          25,1  43,6  2,75  51,2
--   C3. non propose         514 fam  75,3 %          35,2  46,8  2,39  66,7
--
-- LECTURE : le gain est de l'EFFICACITE, pas du taux de reussite. +9,7 pts de decroche
-- au premier essai et -0,34 tentative entre A et C, mais seulement +1,5 pt sur le taux
-- d'appels OK -- soit moins que la dispersion normale du residu (6,3 pts, cf. d08).
-- C2 n'est pas plus dur a joindre : son decroche CONDITIONNEL (43,6) est celui de la
-- ligne de base (42,9). Ce qui s'effondre est la part de familles jointes. C'est un
-- residu negativement selectionne sur l'engagement, pas sur la joignabilite.
--
-- LIMITE : A contre C est un avant/apres brut, confondu avec le pool d'accompagnantes,
-- le canal de recrutement et la saisonnalite. Ne pas le presenter comme un effet causal.
--
-- PARAM : les bornes de periode dans le case final, et callN pour une autre session.
with ycr as (select c.family_id, c.group_id,
        row_number() over (partition by c.family_id order by c.date_birth desc, c.child_id) rn from dim.child c),
y as (select * from ycr where rn=1),
prop0 as (
  select y.family_id, 1 p
  from {{ ref('stg_1001mots_app__events') }} e
  inner join {{ ref('parent_enfant') }} pe on pe.parent_id = e.related_id
  inner join y on y.family_id = pe.child_support_id
  inner join dim.groups g on g.group_id = y.group_id
  cross join lateral (select coalesce(try_to_number(regexp_substr(e.body,'utm_campaign=call([0-3])',1,1,'e',1)),
      case when e.date_occurred <= g.date_call0_end then 0
           when e.date_occurred <= g.date_call1_end then 1
           when e.date_occurred <= g.date_call2_end then 2
           when e.date_occurred <= g.date_call3_end then 3 end) as cs) pr
  where e.type='Events::TextMessage' and e.body ilike '%utm_source=1001mots%' and pr.cs = 0
  group by 1),
ac as (select child_support_id fid, count(*) tent,
              max(iff(rn=1, duration, null)) d1
       from (select *, row_number() over (partition by child_support_id order by started_at) rn
             from staging.stg_1001mots_app__aircall_calls
             where call_session=0 and direction='outbound') group by 1),
fam as (
  select st.family_id, left(st.cohort_name,10) coh, max(st.call_0_status) stt,
         max(st.nb_of_tries_call0) tries, max(st.has_booking_call0) rdv,
         max(g.date_call0_end) fin
  from fact.super_table st
  inner join dim.child c on c.child_id=st.child_id
  inner join dim.groups g on g.group_id=c.group_id
  where st.cohort_name >= '2025/01' group by 1,2),
f as (select fam.*, coalesce(p.p,0) prop, a.tent, a.d1
      from fam left join prop0 p on p.family_id=fam.family_id
               left join ac a on a.fid=fam.family_id
      where fin <= current_date and nullif(trim(stt),'') is not null)
select case when coh < '2026/02' then 'A. avant dispositif'
            when coh < '2026/05' then 'B. demarrage'
            when prop = 1 and rdv = 1 then 'C1. regime - RDV pris'
            when prop = 1             then 'C2. regime - propose sans RDV'
            else                           'C3. regime - non propose' end            as periode,
    count(*)                                                                         as familles,
    round(100.0 * avg(prop), 1)                                                      as pct_propose,
    round(100.0 * count_if(tries is not null) / count(*), 1)                         as pct_jointes,
    round(100.0 * count_if(tries = '1ère tentative') / count(*), 1)                  as decroche_brut,
    round(100.0 * count_if(tries = '1ère tentative')
          / nullif(count_if(tries is not null), 0), 1)                               as decroche_conditionnel,
    round(avg(tent), 2)                                                              as tentatives_aircall,
    round(100.0 * count_if(stt = 'OK') / count(*), 1)                                as pct_ok
from f
group by 1
order by 1
