-- d15 | Ou se situe exactement le trou de 2026/04/20, et combien de temps dure-t-il ?
--
-- DEUX FAITS NOUVEAUX (2026-10-02), les plus contraignants du dossier.
--
-- (A) 2026/04/20 REMONTE en session 1. Memes familles, memes numeros, memes
-- accompagnantes, 13 jours plus tard :
--       S0 = 49,0 %  (fenetre 20/04 -> 03/05)
--       S1 = 60,2 %  (fenetre 18/05 -> 31/05)  <- la moyenne S1 de l'annee est 61,1
--       S2 = 52,7 %
-- Une propriete du fichier (numeros perimes, familles non consentantes) persisterait.
-- Elle ne persiste pas. Le trou est un EPISODE sur la fenetre de premier contact,
-- pas un attribut de la cohorte. A l'inverse 2026/04/06 s'enfonce en S1 (57,3 -> 46,3) :
-- les deux cohortes d'avril n'ont PAS le meme profil, ne pas les traiter en bloc.
--
-- (B) L'effondrement est UNIFORME sur les trois groupes du dispositif RDV. Contre
-- 2026/05/04, son jumeau (meme taux de proposition 52,7 vs 52,9, meme taux de RDV
-- pris 20,7 vs 21,3, fenetre suivante) :
--   | Groupe              | n (04/20) | poids  | OK 04/20 | OK 05/04 | contribution |
--   | RDV pris            |        62 | 21,3 % |   69,4 % |   90,7 % |     -4,5 pts |
--   | Propose sans RDV    |        96 | 33,0 % |   43,8 % |   57,0 % |     -4,4 pts |
--   | Non propose         |       133 | 45,7 % |   43,6 % |   67,1 % |    -10,7 pts |
--                                                            total     -19,6 pts
-- Lecture : la MASSE de l'ecart est dans le groupe non propose, qui n'a aucun
-- rapport avec le dispositif. Le mauvais fonctionnement des RDV ne peut porter que
-- ~4,5 points sur 19,6. Ne pas presenter le RDV comme l'explication de 2026/04/20.
--
-- Cela dit, 69,4 % chez des familles qui ont CHOISI leur creneau est l'anomalie la
-- plus nette du dossier (90,0 a 91,7 % sur 04/06, 05/04, 05/18). Ce qui a ete
-- verifie, et ecarte, sur ce groupe :
--   * les creneaux ont bien ete appeles : 77,3 % d'appel a +/- 20 min du creneau,
--     le MEILLEUR taux du printemps, et 87,9 % d'appel le jour meme.
--   * les SMS de rappel sont au niveau normal : 8,1 % de rappel avant le creneau et
--     16,1 % dans les 30 h, contre 8,2 % / 21,9 % pour 2026/05/04.
--   * taux d'annulation 16,7 %, normal (2026/04/06 est a 33,3 % mais sur n = 24).
-- Reste donc des no-shows au creneau choisi. Les 8 transcriptions disponibles sur
-- ces echecs montrent des parents au travail ou avec le bebe au moment du creneau,
-- une confusion sur l'horaire reserve, et un cas d'accompagnante en retard dont
-- l'entretien passe en "Incomplet". Piste operationnelle plausible pendant la
-- montee en charge du dispositif (66 RDV au call 0 contre 30 et 13-17 avant),
-- mais n = 8 : c'est une hypothese, pas un resultat.
--
-- Chaque bloc est autonome.

-- (1) Trajectoire d'une cohorte sur ses quatre sessions, avec les fenetres reelles.
-- Sert aussi de test calendaire CORRECT : trier par `deb` et comparer les cellules
-- qui partagent la meme fenetre. Attention, c'est la ou l'ancien test (d02) etait
-- faible : il opposait 2026/04/20 S0 a 2026/03/23 S1 sur les memes semaines. Or un
-- appel de session 1 est un appel chaud (la famille connait son accompagnante, a son
-- numero), un appel de session 0 est un premier contact. Une periode de vacances ou
-- de ponts peut peser sur le second sans toucher le premier -- "memes semaines,
-- resultats opposes" ne suffit donc pas a tuer le calendrier.
-- Refait proprement : sur la fenetre 04/05 -> 17/05, 2026/05/04 S0 (un premier
-- contact, en plein pont du 1er et du 8 mai) fait 68,5 %, le meilleur du printemps,
-- pendant que 2026/04/06 S1 fait 46,3 %. Le calendrier reste ECARTE, maintenant sur
-- la bonne comparaison.
with base as (
    select st.family_id, st.cohort_name,
        st.call_0_status, st.call1_status, st.call2_status, st.call3_status,
        g.date_call0_start::date s0, g.date_call0_end::date e0,
        g.date_call1_start::date s1, g.date_call1_end::date e1,
        g.date_call2_start::date s2, g.date_call2_end::date e2,
        g.date_call3_start::date s3, g.date_call3_end::date e3
    from fact.super_table st
    inner join dim.child c on c.child_id = st.child_id
    inner join dim.groups g on g.group_id = c.group_id
    where st.cohort_name like '2026/%'),
sess as (
    select family_id, cohort_name, 0 cs, call_0_status stt, s0 deb, e0 fin from base
    union all select family_id, cohort_name, 1, call1_status, s1, e1 from base
    union all select family_id, cohort_name, 2, call2_status, s2, e2 from base
    union all select family_id, cohort_name, 3, call3_status, s3, e3 from base),
fs as (
    select family_id, cohort_name, cs, max(stt) stt, max(deb) deb, max(fin) fin
    from sess group by 1,2,3)
select left(cohort_name,10) coh, cs, deb, fin,
       count_if(nullif(trim(stt),'') is not null) n,
       round(100.0*count_if(stt='OK')/nullif(count_if(nullif(trim(stt),'') is not null),0),1) pct_ok
from fs
where fin <= current_date
group by 1,2,3,4
having n >= 50
order by deb

;

-- (2) Contribution de chaque groupe RDV a l'ecart, en points de cohorte
-- PARAM : la cohorte evaluee et sa cohorte de reference.
with b as (
  select left(cohort_name,10) coh,
         case when has_booking_call0 = 1 then '1. RDV pris'
              when has_booking_proposal_call0 = 1 then '2. propose sans RDV'
              else '3. non propose' end grp,
         iff(call_0_status='OK',1,0) ok
  from fact.super_table
  where left(cohort_name,10) in ('2026/04/20','2026/05/04')
    and nullif(trim(call_0_status),'') is not null),
a as (
  select grp, count_if(coh='2026/04/20') n_cible,
         avg(iff(coh='2026/04/20', ok, null)) ok_cible,
         avg(iff(coh='2026/05/04', ok, null)) ok_ref
  from b group by 1)
select grp, n_cible,
       round(100.0*n_cible/sum(n_cible) over (),1) poids,
       round(100.0*ok_cible,1) ok_cible,
       round(100.0*ok_ref,1) ok_ref,
       round(100.0*(ok_cible-ok_ref)*n_cible/sum(n_cible) over (),1) contrib_pts
from a order by 1

;

-- (3) Les creneaux de RDV ont-ils ete honores, et les familles etaient-elles la ?
with fam as (
  select family_id, left(cohort_name,10) coh, call_0_status stt
  from fact.super_table
  where cohort_name like '2026/%' and left(cohort_name,10) between '2026/02/23' and '2026/09/07'
    and nullif(trim(call_0_status),'') is not null),
sc as (
  select s.family_id::int fid, s.scheduled_at, s.canceled_at
  from staging.stg_1001mots_app__scheduled_calls s
  where s.call_session = 0),
ac as (
  select child_support_id fid, started_at, duration
  from staging.stg_1001mots_app__aircall_calls
  where call_session = 0 and direction = 'outbound'),
j as (
  select f.coh, f.family_id, f.stt, s.scheduled_at, s.canceled_at,
         max(iff(abs(datediff(minute, s.scheduled_at, a.started_at)) <= 20, 1, 0)) appel_sur_creneau,
         max(iff(abs(datediff(minute, s.scheduled_at, a.started_at)) <= 20
                 and a.duration >= 300, 1, 0)) conv_sur_creneau,
         max(iff(a.started_at::date = s.scheduled_at::date, 1, 0)) appel_meme_jour
  from fam f
  join sc s on s.fid = f.family_id
  left join ac a on a.fid = f.family_id
  group by 1,2,3,4,5)
select coh, count(*) n_rdv,
       round(100.0*count_if(canceled_at is not null)/count(*),1) pct_annule,
       round(100.0*avg(appel_sur_creneau),1) pct_appel_creneau,
       round(100.0*avg(conv_sur_creneau),1) pct_conv_creneau,
       round(100.0*avg(appel_meme_jour),1) pct_appel_jour,
       round(100.0*count_if(stt='OK')/count(*),1) pct_ok
from j group by 1 order by 1
