-- d05 | Comparer la composition des familles d'un groupe de cohortes au reste.
-- Sort les indicateurs tries par ecart absolu decroissant, ce qui fait remonter
-- immediatement ce qui distingue vraiment le groupe.
-- Resultat (avril 2026 vs autres) : pct_caf 96,8 contre 49,2 (+47,6), pct_pmi 3,0
-- contre 44,5, age a l'inscription 17 mois contre 12, bilinguisme 44,9 contre 53,8.
-- ATTENTION : un ecart de composition ne prouve rien tant qu'on n'a pas verifie que
-- la variable DISCRIMINE la performance sur le reste du perimetre. Ici l'age ne
-- discriminait pas : a canal caf identique, avril SUR-performait sur les <12 mois.
-- PARAM : la liste des cohortes du groupe 'CIBLE' et la fenetre de comparaison.

with st as (
    select *,
        iff(left(cohort_name,10) in ('2026/04/06','2026/04/20'),'CIBLE','AUTRES') as grp
    from fact.super_table
    where cohort_name like '2026/%'
      and left(cohort_name,10) between '2026/01/12' and '2026/06/01'
),
ind as (
    select grp, 'pct_caf' as indic, 100.0*count_if(source_channel='caf')/count(*) as v from st group by 1
    union all select grp,'pct_pmi', 100.0*count_if(source_channel='pmi')/count(*) from st group by 1
    union all select grp,'pct_local_partner', 100.0*count_if(source_channel='local_partner')/count(*) from st group by 1
    union all select grp,'pct_bilingue', 100.0*count_if(is_bilingue='Oui')/count(*) from st group by 1
    union all select grp,'pct_reinscription', 100.0*count_if(is_reenrollment::string in ('true','TRUE','1','Oui'))/count(*) from st group by 1
    union all select grp,'delai_inscription_median', median(try_to_number(registration_delay::string)) from st group by 1
    union all select grp,'age_inscription_median', median(try_to_number(age_at_registration::string)) from st group by 1
    union all select grp,'nb_enfants_moyen', avg(number_of_children) from st group by 1
    union all select grp,'pct_3e_tentative_call0', 100.0*count_if(nb_of_tries_call0='3ème tentative')/count(*) from st group by 1
    union all select grp,'pct_rdv_propose_call0', 100.0*count_if(has_booking_proposal_call0=1)/count(*) from st group by 1
    union all select grp,'pct_rdv_pris_call0', 100.0*count_if(has_booking_call0=1)/count(*) from st group by 1
    union all select grp,'familles_par_accompagnante', count(*)/count(distinct supporter_name) from st group by 1
    union all select grp,'pct_desengage_T1', 100.0*count_if(is_desengage_T1=1)/count(*) from st group by 1
)
select indic,
       round(max(iff(grp='CIBLE', v, null)),1)       as cible,
       round(max(iff(grp='AUTRES', v, null)),1) as autres,
       round(max(iff(grp='CIBLE', v, null)) - max(iff(grp='AUTRES', v, null)),1) as ecart
from ind group by 1 order by abs(ecart) desc
