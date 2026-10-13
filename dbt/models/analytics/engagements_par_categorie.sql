{{ config(materialized='view') }}

-- Ventilation d'une annee d'engagement entre les millesimes : une ligne par
-- projet, annee et categorie, avec la regle de classement en clair et les
-- bornes de dates qui la justifient. De quoi verifier un classement sans
-- ouvrir le modele.
--
-- Deux unites, qui ne se confondent pas :
--   nb_enfants   la portee, un enfant accompagne au moins un jour
--   quota_places les places consommees, au prorata du cycle de 174 jours
--
-- Les categories A a D somment a la portee de l'annee ; E est hors total.
--
-- Pas de pourcentage ici : une part calculee en fenetre dans une vue change de
-- sens selon le filtre de l'appelant. Le rapport se fait dans l'outil de BI, ou
-- se lit dans engagements_par_projet.

with enfants as (
    select *
    from {{ ref('suivi_engagements_enfant') }}
    where not is_source_test
      and not is_cohorte_exclue
)

select
    annee_suivi                                         as annee,
    project_code,
    categorie_suivi                                     as categorie,
    rattachement_temporel                               as rattachement,

    case rattachement_temporel
        when 'annee_precedente' then 'cycle commence avant le 1er janvier et non termine a cette date'
        when 'annee_courante'   then 'cycle commence dans l''annee'
        when 'annee_suivante'   then 'inscrit dans l''annee, cohorte planifiee sur l''annee suivante'
        when 'non_planifie'     then 'inscrit dans l''annee, en attente, aucune cohorte planifiee'
        when 'non_accompagne'   then 'inscrit dans l''annee, aucun jour de cycle dedans (hors total)'
    end                                                 as regle,
    rattachement_temporel != 'non_accompagne'           as compte_dans_la_portee,

    -- portee
    count(*)                                            as nb_enfants,
    count(distinct family_id)                           as nb_familles,

    -- comptabilite
    round(sum(quota_annee), 1)                          as quota_places,
    round(sum(quota_annee_a_date), 1)                   as quota_places_a_date,
    round(avg(quota_annee), 3)                          as quota_moyen_par_enfant,

    -- de quoi verifier le classement
    min(date_inscription)                               as premiere_inscription,
    max(date_inscription)                               as derniere_inscription,
    min(date_debut_accompagnement)                      as premier_debut_cycle,
    max(date_debut_accompagnement)                      as dernier_debut_cycle,
    min(date_fin_quota)                                 as premiere_fin_cycle,
    max(date_fin_quota)                                 as derniere_fin_cycle,
    round(avg(nb_jours_suivi_dans_annee))               as jours_moyens_dans_annee,
    round(avg(nb_jours_cycle))                          as jours_moyens_de_cycle,
    count_if(is_inscrit_dans_annee)                     as dont_inscrits_dans_annee,
    count_if(is_mesurable)                              as dont_mesurables

from enfants
group by
    annee_suivi,
    project_code,
    categorie_suivi,
    rattachement_temporel
