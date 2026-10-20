{{ config(materialized='view') }}

-- Vue d'exploitation du suivi Fidelity : une ligne par annee et par indicateur,
-- colonnes ordonnees de la synthese vers le detail et nommees pour etre lues
-- sans documentation.
--
-- De gauche a droite :
--   1. le verdict     l'annee, l'indicateur, la valeur, la cible, l'ecart
--   2. la maturite    ce qui reste a bouger avant que l'annee soit definitive
--   3. l'assiette     sur combien d'enfants le taux est calcule
--   4. le contrat     la definition retenue et le libelle depose
--
-- Pivoter indicateur en lignes et annee en colonnes redonne exactement la
-- grille Fidelity, valeurs mesurees a la place des cibles. Trier par
-- ecart_points pour savoir quel indicateur decroche.
--
-- Aucune annee n'est filtree ici : le filtre appartient a l'appelant. 2023 sort
-- avec des cibles nulles, elle precede le contrat et sert de comparaison.
--
-- Vue, et non table : elle ne fait que renommer et reordonner
-- fact.suivi_fidelity, ou vivent les regles de calcul.

select
    -- ---------------------------------------------------------------- 1. verdict
    annee,
    indicateur_libelle                                  as indicateur,
    valeur_pct                                          as valeur,
    cible_pct                                           as cible,
    ecart_points,
    -- L'annee a ses cohortes planifiees avant d'avoir ses mesures : les groupes
    -- 2027 existent deja dans l'application. Sans ce premier cas, une annee
    -- encore vide sortirait « En retard » -- un retard sur un accompagnement qui
    -- n'a pas commence.
    case
        when valeur_pct is null then 'Pas encore mesuree'
        when is_baseline        then 'Baseline'
        when cible_pct is null  then 'Hors contrat'
        when is_cible_atteinte  then 'Cible atteinte'
        when is_annee_partielle then 'En retard, annee en cours'
        else                         'En retard'
    end                                                 as statut,

    -- --------------------------------------------------------------- 2. maturite
    is_annee_partielle                                  as annee_en_cours,
    pct_cycle_acheve                                    as pct_enfants_cycle_termine,

    -- --------------------------------------------------------------- 3. assiette
    numerateur                                          as enfants_au_numerateur,
    denominateur                                        as enfants_mesures,
    nb_enfants_cohorte                                  as enfants_de_la_cohorte,
    taux_couverture_mesure                              as pct_cohorte_mesuree,

    -- ---------------------------------------------------------------- 4. contrat
    indicateur_definition                               as definition,
    indicateur_libelle_fidelity                         as libelle_contractuel,
    indicateur_ordre,
    indicateur_code

from {{ ref('suivi_fidelity') }}
