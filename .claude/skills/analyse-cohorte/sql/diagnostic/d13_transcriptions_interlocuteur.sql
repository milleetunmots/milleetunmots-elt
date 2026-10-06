-- d13 | LE test decisif du dossier avril : qui est au bout du fil ?
-- Les transcriptions Aircall sont alimentees depuis avril 2026 (1 291 appels ce
-- mois-la). La question ouverte depuis septembre 2026 etait : sur les echanges de
-- 1 a 2 minutes des cohortes d'avril, l'interlocuteur est-il le parent attendu
-- (familles peu engagees) ou un tiers (numeros reattribues) ?
--
-- VERDICT (2026-10-02) : ni l'un ni l'autre. Ces appels sont des REPONDEURS.
-- Les transcriptions permettent de detecter la messagerie vocale sans ambiguite,
-- ce que la duree seule ne pouvait pas faire. Resultats :
--   * a duree d'appel egale, le taux d'appels ou un humain repond est IDENTIQUE ou
--     SUPERIEUR en avril : 13,1 % contre 15,6 % sur 20-60 s, 32,8 vs 36,5 sur
--     60-120 s, 94,4 vs 78,7 sur 2-5 min, 98,8 vs 90,7 au-dela. Tout l'ecart
--     agrege (36,9 % contre 43,1 % de tentatives atteignant un humain) est un
--     effet de COMPOSITION : avril a moins d'appels longs, pas de mauvais numeros.
--   * quand le parent decroche, c'est le bon parent. Zero cas de tiers dans les
--     36 echanges humains non-OK lus a la main. L'hypothese "numeros reattribues"
--     est donc definitivement ECARTEE (3e confirmation independante, cf. d10 et d14).
--   * ce que les echanges non-OK montrent a la place : le parent decroche, dit
--     qu'il est au travail ou avec le bebe, convient d'un rappel -- et l'entretien
--     n'a jamais lieu dans la fenetre de 13 jours.
--
-- L'entonnoir d'avril fuit a chaque etage, modestement, et ca se cumule :
--   | Groupe | familles jointes | dont sans entretien | OK apres report | OK si entretien |
--   | AVRIL  |           62,9 % |              32,7 % |          41,7 % |          90,5 % |
--   | AUTRES |           68,2 % |              23,9 % |          47,0 % |          96,5 % |
--
-- /!\ DEUX PIEGES, lire avant de relancer :
-- (1) La couverture des transcriptions CROIT avec la duree (0 % sous 20 s, 11-16 %
--     sur 20-60 s, 24-31 % au-dela de 5 min). L'echantillon sur-represente donc les
--     appels longs et gonfle tous les taux d'OK. Pour un chiffre representatif il
--     FAUT reponderer par la distribution reelle des durees -- bloc (3). A couverture
--     comparable entre avril et le reste dans chaque classe, les comparaisons
--     intra-classe sont valides, les comparaisons de niveau brut ne le sont pas.
-- (2) Les messages de repondeur depassent souvent 80 caracteres ("Je ne suis pas la,
--     laissez-moi un message. Une fois votre message enregistre..."). Un classificateur
--     fonde sur la seule longueur du texte externe les compte comme des humains. La
--     liste de marqueurs ci-dessous a ete calibree en lisant les transcriptions ;
--     ne pas la raccourcir.
--
-- Validation du classificateur : conversation humaine -> 84,5 % d'appels OK,
-- repondeur seul -> 40,4 %. Il separe bien.
--
-- Chaque bloc est autonome : le lancer separement.
-- PARAM : les cohortes evaluees, ligne 'AVRIL'.

-- (1) Couverture des transcriptions par cohorte (a verifier avant tout)
with fam as (
  select family_id, left(cohort_name,10) coh
  from fact.super_table
  where cohort_name like '2026/%' and left(cohort_name,10) between '2026/01/12' and '2026/09/07'
),
ac as (
  select a.child_support_id fid, a.duration d, a.raw_transcription_payload p
  from MOTS_DL.MOTS_APP.aircall_calls a
  where a.call_session = 0 and a.direction = 'outbound')
select f.coh, count(*) tentatives,
       count_if(a.p is not null) nb_transcrit,
       round(100.0*count_if(a.p is not null)/count(*),1) pct_transcrit
from fam f join ac a on a.fid = f.family_id
group by 1 order by 1

;

-- (2) Classificateur repondeur / humain, valide sur le statut d'appel
with fam as (
  select family_id, left(cohort_name,10) coh, call_0_status stt,
         iff(left(cohort_name,10) in ('2026/04/06','2026/04/20'),'AVRIL','AUTRES') grp
  from fact.super_table
  where cohort_name like '2026/%' and left(cohort_name,10) between '2026/01/12' and '2026/09/07'
    and nullif(trim(call_0_status),'') is not null),
tr as (
  select t.call_id, f.family_id, f.grp, f.stt, min(t.duration) d,
         lower(translate(listagg(iff(t.participant_type='external', t.text, ''), ' ')
               within group (order by t.start_time),
               'àâäéèêëîïôöùûüç''’-', 'aaaeeeeiioouuuc   ')) ext,
         sum(iff(t.participant_type='external', length(t.text), 0)) ext_len
  from staging.stg_1001mots_app__aircall_call_transcriptions t
  join fam f on f.family_id = t.child_support_id
  where t.call_session = 0 and t.direction='outbound' and t.text is not null
  group by 1,2,3,4),
cl as (
  select *,
   (ext like '%messagerie%' or ext like '%repondeur%' or ext like '%apres le bip%'
    or ext like '%signal sonore%' or ext like '%bip%'
    or ext like '%laisser un message%' or ext like '%laisser votre message%'
    or ext like '%laissez un message%' or ext like '%laissez moi un message%'
    or ext like '%laissez votre message%' or ext like '%message enregistre%'
    or ext like '%tapez diese%' or ext like '%enregistrer votre nom%'
    or ext like '%motif de votre appel%' or ext like '%ne quittez pas%'
    or ext like '%n est pas disponible%' or ext like '%pas joignable%'
    or ext like '%je ne suis pas la%' or ext like '%boite vocale%'
    or ext like '%vous pouvez raccrocher%') vocal
  from tr),
f2 as (
  select family_id, grp, stt,
         max(iff(not vocal and ext_len >= 60, 1, 0)) humain,
         max(iff(not vocal and ext_len >= 60 and d >= 300, 1, 0)) entretien
  from cl group by 1,2,3)
select grp, count(*) n_fam,
       round(100.0*avg(humain),1) pct_joint,
       round(100.0*count_if(humain=1 and entretien=0)/nullif(count_if(humain=1),0),1) pct_report,
       round(100.0*count_if(humain=1 and entretien=0 and stt='OK')
             /nullif(count_if(humain=1 and entretien=0),0),1) ok_apres_report,
       round(100.0*count_if(entretien=1 and stt='OK')/nullif(count_if(entretien=1),0),1) ok_si_entretien,
       round(100.0*count_if(humain=0 and stt='OK')/nullif(count_if(humain=0),0),1) ok_si_jamais_joint
from f2 group by 1 order by 1

;

-- (3) Taux d'humain REPONDERE sur la distribution reelle des durees
-- C'est ce bloc qui donne le chiffre publiable : 36,9 % (avril) contre 43,1 %.
-- Noter que la colonne pct_humain_classe est la vraie information : elle montre
-- qu'avril n'est jamais en dessous a duree egale.
with fam as (
  select family_id, left(cohort_name,10) coh,
         iff(left(cohort_name,10) in ('2026/04/06','2026/04/20'),'AVRIL','AUTRES') grp
  from fact.super_table
  where cohort_name like '2026/%' and left(cohort_name,10) between '2026/01/12' and '2026/09/07'
    and nullif(trim(call_0_status),'') is not null),
cls as (
  select a.id::string cid, f.grp,
         case when a.duration < 20 then 'a' when a.duration < 60 then 'b'
              when a.duration < 120 then 'c' when a.duration < 300 then 'd'
              else 'e' end classe
  from MOTS_DL.MOTS_APP.aircall_calls a
  join fam f on f.family_id = a.child_support_id
  where a.call_session = 0 and a.direction = 'outbound'),
tx as (
  select t.call_id,
         lower(translate(listagg(iff(t.participant_type='external', t.text, ''), ' ')
               within group (order by t.start_time),
               'àâäéèêëîïôöùûüç''’-', 'aaaeeeeiioouuuc   ')) ext,
         sum(iff(t.participant_type='external', length(t.text), 0)) ext_len
  from staging.stg_1001mots_app__aircall_call_transcriptions t
  where t.call_session = 0 and t.direction='outbound' and t.text is not null
  group by 1),
flag as (
  select c.grp, c.classe,
         iff(not (x.ext like '%messagerie%' or x.ext like '%repondeur%' or x.ext like '%bip%'
              or x.ext like '%signal sonore%' or x.ext like '%apres le bip%'
              or x.ext like '%laisser un message%' or x.ext like '%laisser votre message%'
              or x.ext like '%laissez un message%' or x.ext like '%laissez moi un message%'
              or x.ext like '%laissez votre message%' or x.ext like '%message enregistre%'
              or x.ext like '%tapez diese%' or x.ext like '%enregistrer votre nom%'
              or x.ext like '%motif de votre appel%' or x.ext like '%ne quittez pas%'
              or x.ext like '%n est pas disponible%' or x.ext like '%pas joignable%'
              or x.ext like '%je ne suis pas la%' or x.ext like '%boite vocale%'
              or x.ext like '%vous pouvez raccrocher%')
              and x.ext_len >= 60, 1, 0) humain
  from cls c join tx x on x.call_id = c.cid),
p as (select grp, classe, count(*) n_tr, avg(humain) p_hum from flag group by 1,2),
w as (select grp, classe, count(*) n_tot from cls group by 1,2)
select w.grp, w.classe, w.n_tot, coalesce(p.n_tr,0) n_tr,
       round(100.0*coalesce(p.p_hum,0),1) pct_humain_classe,
       round(100.0*sum(w.n_tot*coalesce(p.p_hum,0)) over (partition by w.grp)
             / sum(w.n_tot) over (partition by w.grp),1) pct_humain_repondere
from w left join p on p.grp=w.grp and p.classe=w.classe
order by 1,2

;

-- (4) Lecture a la main des echanges humains non-OK. Indispensable : c'est ce bloc
-- qui a fait tomber l'hypothese du mauvais interlocuteur et revele le motif du
-- rappel convenu. Lancer avec --output json, sinon le texte est tronque a 20 car.
with fam as (
  select family_id, left(cohort_name,10) coh, call_0_status stt
  from fact.super_table
  where left(cohort_name,10) in ('2026/04/06','2026/04/20')
    and nullif(trim(call_0_status),'') is not null and call_0_status <> 'OK'),
tr as (
  select t.call_id, f.coh, f.stt, min(t.duration) d,
         listagg(case when t.participant_type='internal' then 'A: ' else 'P: ' end || t.text, ' // ')
               within group (order by t.start_time) conv,
         lower(translate(listagg(iff(t.participant_type='external', t.text, ''), ' ')
               within group (order by t.start_time), 'àâäéèêëîïôöùûüç''’-', 'aaaeeeeiioouuuc   ')) ext,
         sum(iff(t.participant_type='external', length(t.text), 0)) ext_len
  from staging.stg_1001mots_app__aircall_call_transcriptions t
  join fam f on f.family_id = t.child_support_id
  where t.call_session = 0 and t.direction='outbound' and t.text is not null
  group by 1,2,3)
select coh, stt, d, left(conv, 1400) conv
from tr
where ext_len >= 60
  and not (ext like '%messagerie%' or ext like '%repondeur%' or ext like '%bip%'
     or ext like '%signal sonore%' or ext like '%laisser un message%'
     or ext like '%laissez un message%' or ext like '%laissez moi un message%'
     or ext like '%laissez votre message%' or ext like '%message enregistre%'
     or ext like '%tapez diese%' or ext like '%enregistrer votre nom%'
     or ext like '%motif de votre appel%' or ext like '%ne quittez pas%'
     or ext like '%n est pas disponible%' or ext like '%je ne suis pas la%'
     or ext like '%boite vocale%' or ext like '%vous pouvez raccrocher%')
order by d
