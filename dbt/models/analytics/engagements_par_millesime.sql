{{ config(materialized='view') }}

-- Croisement des deux questions que l'on confond : d'ou vient l'enfant compte
-- sur l'annee, et ou tombe reellement la majorite de son cycle.
--
-- Un enfant entre en novembre est bien une entree de l'annee -- il compte dans
-- sa portee -- mais 85 % de son cycle tombe l'annee suivante. Une ligne par
-- projet, annee, provenance et millesime d'atterrissage.
--
-- Le cycle d'un enfant pese une place, repartie entre les annees au prorata des
-- jours : la somme de ses quota par annee fait au plus 1, moins s'il a decroche.
-- Les colonnes places_en_n_moins_1 / n / n_plus_1 sont relatives a la colonne
-- annee de la ligne, pas a une annee civile fixe.

with enfants as (
    select *
    from {{ ref('suivi_engagements_enfant') }}
    where not is_source_test
      and not is_cohorte_exclue
),

-- les enfants comptes dans la portee de chaque annee
comptes_annee as (
    select
        project_code,
        annee_suivi as annee,
        child_id,
        rattachement_temporel
    from enfants
    where rattachement_temporel != 'non_accompagne'
),

-- les memes enfants, vus sur les trois millesimes autour de leur annee
quota_par_millesime as (
    select
        c.project_code,
        c.annee,
        c.child_id,
        c.rattachement_temporel,
        sum(case when t.annee_suivi = c.annee - 1 then t.quota_annee else 0 end) as quota_n_moins_1,
        sum(case when t.annee_suivi = c.annee     then t.quota_annee else 0 end) as quota_n,
        sum(case when t.annee_suivi = c.annee + 1 then t.quota_annee else 0 end) as quota_n_plus_1
    from comptes_annee as c
    join enfants as t
        on t.child_id = c.child_id
    group by c.project_code, c.annee, c.child_id, c.rattachement_temporel
)

select
    annee,
    project_code,
    rattachement_temporel                               as vient_de,

    case
        when quota_n_moins_1 + quota_n + quota_n_plus_1 = 0
            then '4. cycle pas commence'
        when quota_n_moins_1 >= greatest(quota_n, quota_n_plus_1)
            then '1. cycle surtout en N-1'
        when quota_n_plus_1 > greatest(quota_n_moins_1, quota_n)
            then '3. cycle surtout en N+1'
        else '2. cycle surtout en N'
    end                                                 as ou_tombe_le_cycle,

    count(*)                                            as nb_enfants,

    -- ce que ces enfants apportent a chaque millesime, en places
    round(sum(quota_n_moins_1), 1)                      as places_en_n_moins_1,
    round(sum(quota_n), 1)                              as places_en_n,
    round(sum(quota_n_plus_1), 1)                       as places_en_n_plus_1,
    round(avg(quota_n), 3)                              as place_moyenne_en_n

from quota_par_millesime
group by
    annee,
    project_code,
    rattachement_temporel,
    ou_tombe_le_cycle
