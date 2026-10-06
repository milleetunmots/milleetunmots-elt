-- d18 | Le dossier 2026/04/06, et le mecanisme d'attrition :
-- LA REGLE DES DEUX APPELS KO CONSECUTIFS.
-- (le nom du fichier dit "session1_charniere", c'etait l'hypothese de depart ; elle a
--  ete corrigee le jour meme, voir l'avertissement du bloc A)
--
-- Etabli le 2026-10-02 en creusant 2026/04/06, la cohorte que l'analyse de septembre
-- masquait en la traitant avec 2026/04/20. La regle metier a ete signalee par Guillaume
-- en cours d'analyse, puis verifiee sur les donnees.
--
-- ========== (A) LE RESULTAT GENERAL : DEUX KO CONSECUTIFS, PAS UNE S1 RATEE ==========
--
-- /!\ CORRECTION DU 2026-10-02, le meme jour. Ce fichier affirmait d'abord qu'une
-- session 1 ratee faisait sortir la famille du parcours (~35 points de couverture).
-- C'ETAIT FAUX, par agregation trompeuse. L'application applique une regle metier :
-- **apres DEUX appels KO consecutifs, la famille est desengagee automatiquement.**
-- Le croisement S0 x S1 le montre sans ambiguite :
--
--   | S0 | S1 | familles | couverture S2 | group_status <> active |
--   | KO | KO |      234 |      26,5 %   |         85,9 %         |
--   | KO | OK |      212 |      94,8 %   |         48,6 %         |
--   | OK | KO |      227 |      98,2 %   |         49,8 %         |
--   | OK | OK |      446 |      98,0 %   |          8,7 %         |
--
-- Un KO ISOLE ne coute rien en couverture : 98,2 % apres (OK, KO) et 94,8 % apres
-- (KO, OK). Seule la SEQUENCE de deux KO declenche la sortie. Le "60 % de couverture
-- apres une S1 ratee" du premier jet etait le melange de ces deux cas tres differents.
-- Lecon de methode : ne jamais conditionner sur le statut d'UNE session quand une regle
-- metier porte sur une sequence. Toujours croiser S0 x S1.
--
-- Le taux d'arret DECLARE (stop_support_reason) reste bas partout : ces familles ne se
-- desinscrivent pas, elles sont desinscrites. L'attrition est administrative.
--
-- Consequence pour le pilotage, et elle est forte : **le cout d'une mauvaise session
-- depend entierement de ce qui s'est passe a la precedente.** Une cohorte qui a rate sa
-- session 0 joue toute sa taille sur la session 1 ; une cohorte qui a reussi sa session 0
-- peut rater la 1 sans perdre personne. La part de DOUBLE KO est donc le bon indicateur
-- de perte, et elle predit la couverture presque parfaitement sur 2026 (bloc 6) :
--   2026/01/26 11,0 % -> 87,4 de couverture S2   2026/05/18 18,8 % -> 77,0
--   2026/06/01 12,6 % -> 81,0                    2026/04/20 23,7 % -> 77,7
--   2026/05/04 15,1 % -> 81,2                    2026/04/06 26,5 % -> 66,4
-- 2026/04/06 est la pire de l'annee sur cet indicateur, et la seule a avoir rate ses
-- DEUX premieres sessions (57,3 puis 46,3). 2026/04/20, dont la session 0 est pourtant
-- bien plus basse (49,0), sauve sa taille parce que sa session 1 revient a 60,2.
--
-- ========== (B) CE QUI EXPLIQUE 2026/04/06 ==========
-- Elle est la seule cohorte de 2026 a avoir rate ses deux premieres sessions (57,3 puis
-- 46,3), donc la seule a envoyer 26,5 % de ses familles dans la regle du double KO.
-- C'est de la que vient la perte de la moitie de la cohorte -- pas d'un desengagement
-- spontane des familles.
--
-- Ou se situe le trou, precisement : 2026/04/06 est composee de CAF Oise (136 familles
-- importees en 2 jours), CAF Aube (66) et CAF 93 (26). Par session :
--   | Source   |  n  |   S0   |   S1   |   S2   |   S3   |
--   | CAF Oise | 133 | 59,4 % | 39,8 % | 64,4 % | 61,5 % |
--   | CAF Aube |  65 | 47,5 % | 40,7 % | 45,0 % | 50,0 % |
--   | CAF 93   |  26 | 61,5 % | 79,2 % | 40,0 % | 61,1 % |
-- CAF Oise fait une session 0 NORMALE puis s'effondre de 19,6 points en S1, et
-- remonte en S2. Le lot d'import massif n'est donc pas mauvais a l'arrivee -- il le
-- devient entre la session 0 et la session 1.
--
-- Ce qui est ECARTE sur ce trou :
--   * artefact PaperTrail sur call1_status : ZERO desaccord entre staging et source
--     sur les 5 cohortes testees (bloc 2). La chute est reelle.
--   * une accompagnante absente : les 133 familles Oise sont reparties sur 12
--     accompagnantes (5 a 31 familles chacune), la baisse est chez presque toutes.
--   * un defaut d'effort : ces familles sont appelees PLUS que les autres --
--     2,71 tentatives sur 2,41 jours pour Oise et 2,83 pour Aube, contre 2,37 a 2,51
--     sur 1,92 a 2,04 pour les cohortes de reference. 11,3 % seulement n'ont jamais
--     ete appelees.
--   * un retard de livraison des livres : le module 2 est attribue a J+28 de l'ouverture
--     de la session 0 dans TOUTES les cohortes, a la journee pres.
--   * le desengagement des familles : c'est l'inverse qui est vrai, voir ci-dessous.
--   * le calendrier : la fenetre S1 de 2026/04/06 est le 04/05 -> 17/05, dans laquelle
--     2026/05/04 S0 fait 68,5 % (le meilleur premier contact du printemps) et
--     2026/03/09 S2 fait 64,9 %. Cf. d02.
--
-- ========== (C) CE QUI SE PASSE REELLEMENT ==========
-- Ces familles ne decrochent pas le telephone ET sont plus engagees que la moyenne.
-- Pendant la fenetre S1, 22,6 % des familles Oise et 23,1 % des Aube envoient un SMS
-- a leur accompagnante, contre 17,4 a 19,0 % dans les cohortes de reference. Zero SMS
-- non delivre. Mais seulement 33,8 % et 36,9 % d'entre elles ont une conversation
-- telephonique de 2 min ou plus, contre 52,3 a 56,6 %.
--
-- LE CHIFFRE QUI RESUME : parmi les familles qui ECRIVENT pendant la fenetre S1,
-- seules 46,7 % obtiennent un appel OK, contre 62,3 a 83,0 % dans les autres cohortes.
-- Ecrire a l'association et ne pas etre rappelee, c'est ca le mode de defaillance.
--
-- La lecture des echanges SMS (bloc 4) montre un motif unique et repete : une chaine
-- de reports qui ne converge jamais dans les 13 jours. Le parent propose un creneau,
-- un imprevu tombe, il en propose un autre, l'accompagnante n'est pas libre, la
-- fenetre se ferme. L'indisponibilite de l'accompagnante y contribue explicitement --
-- "je ne vais pas pouvoir vous appeler d'ici la fin de semaine, je vous rappellerai
-- dans un petit mois maintenant", sur une famille qui venait de confirmer avoir recu
-- le livre et que son enfant l'adorait. Ce n'est ni un probleme de fichier, ni un
-- probleme de famille : c'est un probleme de prise de rendez-vous.
--
-- A rapprocher de d15 : le dispositif RDV repond exactement a ce mecanisme, et c'est
-- la fin du parcours qui l'utilise le plus (32,9 % de RDV pris en S3 contre 20,8 % en
-- S0). Sur 2026/04/06, le taux de proposition au call 0 n'etait que de 35,3 %.
--
-- Chaque bloc est autonome. PARAM : les cohortes, dans le `in (...)` de chaque bloc.

-- (1) LE bloc a lancer en premier : l'attrition selon le statut de la session 1
with base as (
  select st.family_id, left(st.cohort_name,10) coh,
         st.call1_status s1, st.call2_status s2,
         st.stop_support_reason, g.date_call2_end::date f2
  from fact.super_table st
  inner join dim.child c on c.child_id = st.child_id
  inner join dim.groups g on g.group_id = c.group_id
  where left(st.cohort_name,10) in ('2026/03/23','2026/04/06','2026/04/20','2026/05/04')
    and nullif(trim(st.call1_status),'') is not null),
d as (select distinct family_id, coh, s1, s2, stop_support_reason, f2 from base)
select coh, iff(s1='OK','S1 reussie','S1 ratee') grp_s1,
       count(*) familles,
       round(100.0*count_if(nullif(trim(s2),'') is not null)/count(*),1) couverture_s2,
       round(100.0*count_if(s2='OK')/count(*),1) ok_s2_par_inscrite,
       round(100.0*count_if(stop_support_reason is not null)/count(*),1) pct_arret_declare
from d where f2 <= current_date
group by 1,2 order by 1,2

;

-- (2) Controle PaperTrail : le staging perime-t-il call1_status ?
-- A relancer avant de conclure sur toute chute de session, le staging faisant
-- coalesce(object_json, source) ou object_json est l'etat AVANT derniere modification.
-- Resultat 2026-10-02 : zero desaccord sur les 5 cohortes testees.
with st as (
  select family_id, left(cohort_name,10) coh, call1_status stg_s1
  from fact.super_table
  where left(cohort_name,10) in ('2026/03/23','2026/04/06','2026/04/20','2026/05/04','2026/06/01')),
src as (
  select id::string family_id, nullif(trim(call1_status::string),'') src_s1
  from MOTS_DL.MOTS_APP.child_supports)
select st.coh, count(*) familles,
       count_if(coalesce(nullif(trim(st.stg_s1),''),'~') <> coalesce(s.src_s1,'~')) desaccords,
       round(100.0*count_if(st.stg_s1='OK')
             /nullif(count_if(nullif(trim(st.stg_s1),'') is not null),0),1) ok_staging,
       round(100.0*count_if(s.src_s1='OK')/nullif(count_if(s.src_s1 is not null),0),1) ok_source
from st join src s on s.family_id = st.family_id
group by 1 order by 1

;

-- (3) Effort d'appel et joignabilite en session 1, 2026/04/06 eclate par source.
-- Montre que les familles sont appelees PLUS et joignables MOINS.
with fam as (
  select distinct st.family_id, left(st.cohort_name,10) coh, st.source_name,
         g.date_call1_start::date deb
  from fact.super_table st
  inner join dim.child c on c.child_id = st.child_id
  inner join dim.groups g on g.group_id = c.group_id
  where left(st.cohort_name,10) in ('2026/03/23','2026/04/06','2026/04/20','2026/05/04')),
grp as (
  select family_id, deb,
         iff(coh='2026/04/06', coh || ' / ' || coalesce(source_name,'?'), coh) cle
  from fam),
ac as (
  select child_support_id fid, started_at, started_at::date jour,
         iff(datediff(second, answered_at, ended_at) >= 120, 1, 0) conv
  from MOTS_DL.MOTS_APP.aircall_calls
  where call_session = 1 and direction = 'outbound'),
j as (
  select g.cle, g.family_id, count(a.started_at) tentatives,
         count(distinct a.jour) jours, max(a.conv) a_converse
  from grp g left join ac a on a.fid = g.family_id
  group by 1,2)
select cle, count(*) familles,
       round(100.0*count_if(tentatives=0)/count(*),1) pct_jamais_appelee,
       round(avg(iff(tentatives>0,tentatives,null)),2) tentatives_moy,
       round(avg(iff(tentatives>0,jours,null)),2) jours_moy,
       round(100.0*avg(coalesce(a_converse,0)),1) pct_fam_avec_conv
from j group by 1 having familles >= 20 order by 1

;

-- (4) LE bloc qui a fait basculer le diagnostic : les familles qui ECRIVENT
-- pendant la fenetre aboutissent-elles a un appel OK ?
with fam as (
  select distinct st.family_id, left(st.cohort_name,10) coh, st.source_name,
         st.call1_status stt, g.date_call1_start::date deb, g.date_call1_end::date fin
  from fact.super_table st
  inner join dim.child c on c.child_id = st.child_id
  inner join dim.groups g on g.group_id = c.group_id
  where left(st.cohort_name,10) in ('2026/03/23','2026/04/06','2026/04/20','2026/05/04','2026/06/01')
    and nullif(trim(st.call1_status),'') is not null),
grp as (
  select family_id, stt, deb, fin,
         iff(coh='2026/04/06' and source_name in ('CAF Oise','CAF Aube'),
             '2026/04/06 Oise+Aube', coh) cle
  from fam),
m as (
  select child_support_id fid, direction, created_at::date dt
  from staging.stg_1001mots_app__aircall_messages),
j as (
  select g.cle, g.family_id, g.stt, count_if(m.direction='inbound') entrants
  from grp g left join m on m.fid = g.family_id and m.dt between g.deb and g.fin
  group by 1,2,3)
select cle, count(*) familles,
       count_if(entrants > 0) n_ecrivent,
       round(100.0*count_if(entrants > 0 and stt='OK')/nullif(count_if(entrants > 0),0),1) ok_si_ecrivent,
       round(100.0*count_if(entrants = 0 and stt='OK')/nullif(count_if(entrants = 0),0),1) ok_si_silence
from j group by 1 order by 1

;

-- (5) Lire les echanges SMS des familles engagees dont la session 1 a echoue.
-- Lancer avec --output json, sinon le texte est tronque a 20 caracteres.
with fam as (
  select distinct st.family_id, st.call1_status stt,
         g.date_call1_start::date deb, g.date_call1_end::date fin
  from fact.super_table st
  inner join dim.child c on c.child_id = st.child_id
  inner join dim.groups g on g.group_id = c.group_id
  where left(st.cohort_name,10) = '2026/04/06'
    and st.source_name in ('CAF Oise','CAF Aube')
    and nullif(trim(st.call1_status),'') is not null and st.call1_status <> 'OK'),
m as (
  select child_support_id fid, direction, created_at, body
  from staging.stg_1001mots_app__aircall_messages),
j as (
  select f.family_id, f.stt, count_if(m.direction='inbound') n_in,
         listagg(iff(m.direction='outbound','A> ','P> ') || left(m.body, 260), '  ||  ')
           within group (order by m.created_at) echange
  from fam f join m on m.fid = f.family_id and m.created_at::date between f.deb and f.fin
  group by 1,2)
select family_id, stt, n_in, left(echange, 1500) echange
from j where n_in > 0 order by n_in desc

;

-- (6) LE bloc de reference : part de double KO par cohorte, qui predit la couverture.
-- A preferer au taux d'OK d'une session isolee pour estimer ce qu'une cohorte a coute.
with base as (
  select st.family_id, left(st.cohort_name,10) coh,
         st.call_0_status s0, st.call1_status s1, st.call2_status s2,
         g.date_call1_end::date f1
  from fact.super_table st
  inner join dim.child c on c.child_id = st.child_id
  inner join dim.groups g on g.group_id = c.group_id
  where st.cohort_name like '2026/%' and left(st.cohort_name,10) between '2026/01/12' and '2026/06/01'),
d as (select distinct family_id, coh, s0, s1, s2, f1 from base)
select coh, count(*) familles_inscrites,
       round(100.0*count_if(s0='OK')/nullif(count_if(nullif(trim(s0),'') is not null),0),1) ok_s0,
       round(100.0*count_if(s1='OK')/nullif(count_if(nullif(trim(s1),'') is not null),0),1) ok_s1,
       round(100.0*count_if(nullif(trim(s0),'') is not null and s0<>'OK'
                        and nullif(trim(s1),'') is not null and s1<>'OK')/count(*),1) pct_double_ko,
       round(100.0*count_if(nullif(trim(s2),'') is not null)/count(*),1) couverture_s2
from d where f1 <= current_date
group by 1 order by 1

;

-- (7) Croisement S0 x S1 : LE bloc qui etablit la regle du double KO. A lancer avant
-- toute lecture de l'attrition.
with base as (
  select st.family_id, st.call_0_status s0, st.call1_status s1, st.call2_status s2,
         st.group_status, st.stop_support_reason, g.date_call2_end::date f2
  from fact.super_table st
  inner join dim.child c on c.child_id = st.child_id
  inner join dim.groups g on g.group_id = c.group_id
  where left(st.cohort_name,10) in ('2026/03/23','2026/04/06','2026/04/20','2026/05/04')
    and nullif(trim(st.call_0_status),'') is not null
    and nullif(trim(st.call1_status),'') is not null),
d as (select distinct family_id, s0, s1, s2, group_status, stop_support_reason, f2 from base)
select iff(s0='OK','S0 OK','S0 KO') c0, iff(s1='OK','S1 OK','S1 KO') c1,
       count(*) familles,
       round(100.0*count_if(nullif(trim(s2),'') is not null)/count(*),1) couverture_s2,
       round(100.0*count_if(group_status <> 'active')/count(*),1) pct_non_active,
       round(100.0*count_if(stop_support_reason is not null)/count(*),1) pct_arret_declare
from d where f2 <= current_date
group by 1,2 order by 1,2

;

-- (8) Appariement a la SEMAINE : le deficit est-il attache aux familles, ou a
-- l'accompagnante / a la periode ? Chaque cote ramene a la norme de sa session.
-- Resultat 2026-10-02 : 2026/04/20 S0 a -9,8 quand ses accompagnantes sont a -1,9 sur
-- leurs autres appels des memes semaines (net -7,9) ; 2026/04/06 S1 Oise+Aube a -11,3
-- quand les memes personnes sont a +2,7 (net -14,0). C'est bien attache aux FAMILLES.
-- /!\ Apparier au JOUR est trop serre : on ne retient que ~39 % des appels et l'estimation
-- devient bruitee. A la semaine on garde ~72 %. Et ne JAMAIS comparer des taux bruts entre
-- rangs de session (norme 32,0 % en S0 contre 28,1 en S1, 26,1 en S2).
with fam as (
  select distinct family_id, left(cohort_name,10) coh, source_name from fact.super_table
  where cohort_name like '2026/%'),
ac as (
  select a.caller_id, a.started_at::date jour, date_trunc('week', a.started_at)::date sem,
         a.child_support_id fid, a.call_session cs,
         iff(datediff(second, a.answered_at, a.ended_at) >= 120, 1, 0) conv
  from MOTS_DL.MOTS_APP.aircall_calls a
  where a.direction = 'outbound' and a.started_at >= '2026-01-01'),
norme as (
  select a.cs, avg(a.conv) r from ac a join fam f on f.family_id = a.fid
  where f.coh not in ('2026/04/06','2026/04/20') group by 1),
tagged as (
  select a.caller_id, a.jour, a.sem, a.conv - n.r ecart,
         case when f.coh='2026/04/20' and a.cs=0 then 'CIBLE_A'
              when f.coh='2026/04/06' and a.cs=1
                   and f.source_name in ('CAF Oise','CAF Aube') then 'CIBLE_B'
              else 'AUTRES' end grp
  from ac a join fam f on f.family_id = a.fid join norme n on n.cs = a.cs),
pa as (
  select 'A 2026/04/20 S0 (20/04-03/05)' cible, caller_id, sem,
         count_if(grp='CIBLE_A') n_c, avg(iff(grp='CIBLE_A', ecart, null)) e_c,
         count_if(grp='AUTRES')  n_a, avg(iff(grp='AUTRES',  ecart, null)) e_a
  from tagged where jour between '2026-04-20' and '2026-05-03' group by 1,2,3
  having n_c >= 2 and n_a >= 2),
pb as (
  select 'B 2026/04/06 S1 (04/05-17/05)' cible, caller_id, sem,
         count_if(grp='CIBLE_B') n_c, avg(iff(grp='CIBLE_B', ecart, null)) e_c,
         count_if(grp='AUTRES')  n_a, avg(iff(grp='AUTRES',  ecart, null)) e_a
  from tagged where jour between '2026-05-04' and '2026-05-17' group by 1,2,3
  having n_c >= 2 and n_a >= 2),
p as (select * from pa union all select * from pb)
select cible, count(*) nb_paires, sum(n_c) appels_cible, sum(n_a) appels_autres,
       round(100.0*sum(n_c*e_c)/sum(n_c),1) ecart_norme_cible,
       round(100.0*sum(n_a*e_a)/sum(n_a),1) ecart_norme_autres,
       round(100.0*(sum(n_c*e_c)/sum(n_c) - sum(n_a*e_a)/sum(n_a)),1) ecart_net
from p group by 1 order by 1
