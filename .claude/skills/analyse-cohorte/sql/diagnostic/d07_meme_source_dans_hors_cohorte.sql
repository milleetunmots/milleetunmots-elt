-- d07 | LE test qui distingue "la source est mauvaise" de "le lot est mauvais".
-- Compare chaque source nommee DANS la cohorte evaluee et AILLEURS, a canal constant.
-- Resultat (2026/04/20) : CAF 93 a 47,7 % dans la cohorte contre 60,7 % ailleurs
-- (749 appels), CAF Oise 50,9 contre 59,3, CAF Aube 43,5 contre 55,8. Trois
-- departements differents, trois baisses simultanees de 8 a 13 points.
-- Ce n'est donc ni la CAF en general, ni un partenaire defaillant : c'est ce qui a
-- ete verse dans cette cohorte-la.
-- PARAM : la cohorte evaluee et le canal.
--
-- APPROFONDISSEMENT DU 2026-10-02 -- c'est le zoom le plus productif du dossier, et il
-- corrige en partie la lecture "le deficit est diffus". Il l'est sur les accompagnantes
-- et sur les groupes RDV, il ne l'est PAS sur les sources. Trois ajouts :
--
-- (1) PONDERER. d07 donne des taux, pas des contributions. Or 2026/04/20 est a 69 %
--     CAF 93 (199 familles sur 290). En points de cohorte, contre le niveau de chaque
--     source hors avril :
--       CAF 93    199 fam. (68,6 %)  47,7 contre 60,7  ->  -8,9 pts
--       CAF Oise   53 fam. (18,3 %)  50,9 contre 46,7  ->  +0,8 pt
--       CAF Aube   23 fam. ( 7,9 %)  43,5 contre 61,1  ->  -1,4 pt
--     Soit ~-9,5 points des ~-11,6 de residu portes par une seule livraison. Et dans
--     la MEME cohorte, CAF Oise ne baisse pas -- le deficit est attache aux familles
--     de CAF 93, pas a la cohorte en bloc.
--     /!\ Circularite a garder en tete : quand une source pese 69 % de la cohorte,
--     dire qu'elle sous-performe est presque une reformulation du deficit de la
--     cohorte. L'information est dans le CONTRASTE avec les autres sources du meme
--     lot -- et il repose sur n = 53 et n = 23, donc il est mince.
--
-- (2) LES DEUX COHORTES D'AVRIL N'ONT PAS LA MEME SOURCE COUPABLE :
--       2026/04/06 : CAF Aube 60 fam. a 46,7 contre 61,1 ailleurs. Et sa CAF Oise
--                    (136 fam.) est a 60,3 contre 46,7 ailleurs, soit AU-DESSUS.
--       2026/04/20 : CAF 93 199 fam. a 47,7 contre 60,7.
--     Troisieme raison de ne pas traiter les deux cohortes comme un seul dossier.
--
-- (3) LES SOURCES ONT RECUPERE APRES AVRIL -- c'est la conclusion decisionnelle :
--       CAF 93   : 62,9 avant (350 fam.) / 49,3 en avril (225) / 58,6 apres (377)
--       CAF Aube : 55,6 avant ( 27 fam.) / 45,8 en avril ( 83) / 63,2 apres ( 68)
--       CAF Oise : plus alimentee apres avril (1 famille) -- rien a conclure
--     Les partenariats ne sont pas degrades, il n'y a pas a arreter de recruter la.
--     C'est bien UNE LIVRAISON, pas une source.
--
-- (4) La reprise en session 1 de 2026/04/20 n'est PAS portee par la source : les trois
--     sources remontent ensemble (CAF 93 47,5 -> 57,4, Oise 50,9 -> 66,0, Aube
--     43,5 -> 70,0). Le zoom source localise le trou du premier contact, il n'explique
--     pas sa reversibilite.
--
-- Ce que ce zoom change : la question n'est plus "pourquoi cette cohorte" mais
-- "pourquoi CE fichier de CAF 93". C'est une question qu'on peut poser au partenaire
-- (date de constitution du fichier, modalite d'information des familles), ce que
-- l'autre formulation ne permettait pas. Le bloc (2) ci-dessous produit le tableau.

with b as (
  select st.source_name, st.call_0_status stt,
         iff(left(st.cohort_name,10) = '2026/04/20', 'CIBLE', 'AUTRES') grp
  from fact.super_table st
  where st.cohort_name like '2026/%'
    and st.source_channel = 'caf'
    and left(st.cohort_name,10) between '2026/01/12' and '2026/09/07'
    and nullif(trim(st.call_0_status),'') is not null)
select source_name,
       count_if(grp='CIBLE')                                                        n_cible,
       round(100.0*count_if(grp='CIBLE'  and stt='OK')/nullif(count_if(grp='CIBLE'),0),1)  ok_cible,
       count_if(grp='AUTRES')                                                       n_autres,
       round(100.0*count_if(grp='AUTRES' and stt='OK')/nullif(count_if(grp='AUTRES'),0),1) ok_autres
from b group by 1
having count_if(grp='CIBLE') >= 10
order by 2 desc

;

-- (2) Avant / avril / apres, par source : la source a-t-elle recupere ?
-- PARAM : les bornes de periode.
with b as (
  select st.source_name, iff(st.call_0_status='OK',1,0) ok,
         case when left(st.cohort_name,10) < '2026/04/06' then '1_avant'
              when left(st.cohort_name,10) <= '2026/04/20' then '2_avril'
              else '3_apres' end per
  from fact.super_table st
  where st.cohort_name like '2026/%'
    and left(st.cohort_name,10) between '2026/01/12' and '2026/09/07'
    and nullif(trim(st.call_0_status),'') is not null),
avril as (
  select source_name, count(*) n from b where per='2_avril' group by 1 having n >= 15)
select b.source_name,
       count_if(b.per='1_avant') n_avant,
       round(100.0*avg(iff(b.per='1_avant', b.ok, null)),1) ok_avant,
       count_if(b.per='2_avril') n_avril,
       round(100.0*avg(iff(b.per='2_avril', b.ok, null)),1) ok_avril,
       count_if(b.per='3_apres') n_apres,
       round(100.0*avg(iff(b.per='3_apres', b.ok, null)),1) ok_apres
from b join avril a on a.source_name = b.source_name
group by 1 order by 4 desc

;

-- (3) Source x cohorte d'avril, pour voir que la source coupable n'est pas la meme
select st.source_name,
       count_if(left(st.cohort_name,10)='2026/04/06') n_0406,
       round(100.0*avg(iff(left(st.cohort_name,10)='2026/04/06',
             iff(st.call_0_status='OK',1,0), null)),1) ok_0406,
       count_if(left(st.cohort_name,10)='2026/04/20') n_0420,
       round(100.0*avg(iff(left(st.cohort_name,10)='2026/04/20',
             iff(st.call_0_status='OK',1,0), null)),1) ok_0420,
       round(100.0*avg(iff(left(st.cohort_name,10) not in ('2026/04/06','2026/04/20'),
             iff(st.call_0_status='OK',1,0), null)),1) ok_hors_avril
from fact.super_table st
where st.cohort_name like '2026/%'
  and left(st.cohort_name,10) between '2026/01/12' and '2026/09/07'
  and nullif(trim(st.call_0_status),'') is not null
group by 1
having n_0406 + n_0420 >= 15
order by 4 desc
