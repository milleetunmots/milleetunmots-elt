{{ config(materialized='view') }}

-- Vue projet du suivi des engagements : une ligne par financeur et par annee,
-- colonnes ordonnees de la synthese vers le detail et nommees pour etre lues
-- sans documentation.
--
-- De gauche a droite :
--   1. le verdict        vendu, consomme, avancement, ecart de rythme
--   2. la projection     ce qui est acquis, ce qui est attendu, comment
--   3. la portee         les enfants touches, et d'ou ils viennent
--   4. les places        comment le quota se repartit entre les millesimes
--   5. la qualite        les deux appels OK, sur les seuls enfants mesurables
--   6. les drapeaux      ce qui demande un coup d'oeil
--
-- Aucune annee n'est filtree ici : le filtre appartient a l'appelant. Trier par
-- ecart_points pour savoir qui relancer, par quota_vendu pour lire les gros
-- financeurs d'abord.
--
-- Vue, et non table : elle ne fait que renommer et reordonner
-- fact.suivi_engagements, ou vivent les regles de calcul.

select
    -- ---------------------------------------------------------------- 1. verdict
    annee,
    project_label                                       as projet,
    quota_vendu,
    quota_consomme                                      as places_consommees,
    round(100 * taux_atteinte_quota)                    as pct_quota,
    round(100 * taux_avancement_temps)                  as pct_temps,
    round(100 * ecart_rythme)                           as ecart_points,

    -- ------------------------------------------------------------- 2. projection
    quota_projete_fin_annee                             as places_projetees,
    round(100 * taux_atteinte_quota_projete)            as pct_projete,
    quota_acquis_fin_annee                              as places_acquises,
    nb_entrees_fin_annee_connues                        as entrees_deja_planifiees,
    nb_entrees_attendues_fin_annee                      as entrees_attendues,
    methode_projection,
    quota_projete_bureau                                as projection_du_bureau,

    -- ----------------------------------------------------------------- 3. portee
    nb_enfants_suivis                                   as enfants_touches,
    nb_familles_suivies                                 as familles_touchees,
    round(100 * taux_atteinte_portee)                   as pct_portee,
    nb_enfants_annee_precedente                         as enfants_venus_de_n_moins_1,
    nb_enfants_en_cohorte                               as enfants_en_cohorte,
    nb_familles_en_cohorte                              as familles_en_cohorte,
    round(100 * taux_atteinte_en_cohorte)               as pct_en_cohorte,
    nb_enfants_en_cohorte_projete                       as enfants_en_cohorte_projetes,
    round(100 * taux_atteinte_en_cohorte_projete)       as pct_en_cohorte_projete,
    nb_enfants_annee_courante                           as enfants_entres_dans_annee,
    nb_familles_entrees                                 as familles_entrees_dans_annee,
    round(100 * taux_atteinte_entrees)                  as pct_entrees,
    nb_enfants_annee_suivante                           as enfants_cohorte_n_plus_1,
    nb_enfants_non_planifie                             as enfants_sans_cohorte,
    nb_enfants_non_accompagnes                          as enfants_non_accompagnes,
    nb_enfants_cycle_surtout_annee_suivante             as enfants_suivis_plutot_en_n_plus_1,

    -- ----------------------------------------------------------------- 4. places
    quota_annee_precedente                              as places_des_venus_de_n_moins_1,
    quota_annee_courante                                as places_des_entres_dans_annee,
    round(100 * part_quota_annee_precedente)            as pct_places_de_n_moins_1,
    quota_deja_delivre_annee_precedente                 as places_deja_delivrees_en_n_moins_1,
    quota_reporte_annee_suivante                        as places_reportees_sur_n_plus_1,

    -- ---------------------------------------------------------------- 5. qualite
    nb_enfants_mesurables                               as enfants_mesurables,
    round(100 * taux_deux_appels_ok)                    as pct_deux_appels_ok,
    round(100 * taux_deux_appels_ok_une_pm)             as pct_deux_appels_ok_une_pm,
    nb_enfants_call0_ok                                 as enfants_call0_ok,

    -- --------------------------------------------------------------- 6. drapeaux
    is_hors_engagement,
    is_sans_suivi,
    financeur_type,
    departement,
    project_code

from {{ ref('suivi_engagements') }}
