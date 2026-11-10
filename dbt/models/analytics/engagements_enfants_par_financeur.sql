{{ config(materialized='view') }}

-- Le detail du dashboard « Suivi de projet v2 » : une ligne par enfant et par
-- annee de suivi, pour les seuls financeurs du referentiel dim.financeur.
--
-- LE PERIMETRE EST POSE ICI, UNE FOIS : hors sources de test, hors cohortes
-- exclues des analyses, hors enfants sans financeur suivi (bouche-a-oreille,
-- source non renseignee). analytics.engagements_par_financeur agrege cette vue
-- et rien d'autre : une carte qui lit le detail ne peut donc pas contredire
-- une carte qui lit l'agregat. Dans le premier dashboard, chaque carte de
-- detail devait repeter le filtre des cohortes exclues ; l'oublier comptait
-- 463 entrees de trop (constat du 2026-10-20, en-tete de suivi_projet.yml).
--
-- Les regles de rattachement et la definition d'un « inscrit » vivent dans
-- fact.suivi_financeurs_enfant. Cette vue choisit, renomme et filtre.
--
-- Un enfant apparait sur plusieurs annees : toujours filtrer sur annee.

select
    e.annee_suivi                               as annee,
    f.financeur,
    f.ordre,
    e.child_id,
    e.family_id,
    e.cohorte,

    -- les trois mesures du dashboard, un booleen chacune
    e.is_inscrit_compte_dans_annee              as compte_comme_inscrit,
    e.rattachement_temporel = 'annee_courante'  as est_entre_en_cohorte,
    e.rattachement_temporel = 'annee_precedente' as est_reporte,

    case e.rattachement_temporel
        when 'annee_courante'   then 'Entré en cohorte dans l''année'
        when 'annee_precedente' then 'Reporté de l''année précédente'
        when 'annee_suivante'   then 'Inscrit, cohorte l''année suivante'
        when 'non_planifie'     then 'Inscrit, cohorte non planifiée'
        when 'non_accompagne'   then 'Inscrit, jamais accompagné'
    end                                         as rattachement,

    e.date_inscription,
    e.date_debut_accompagnement,
    e.group_status                              as statut,
    e.source_name                               as source,
    e.source_channel                            as canal,
    e.source_departement                        as departement_source,
    e.famille_departement                       as departement_famille,
    e.child_age_in_month                        as tranche_age,
    e.is_reenrollment                           as est_reinscription,
    e.deux_appels_ok,

    f.financeur_code
from {{ ref('suivi_financeurs_enfant') }} as e
inner join {{ ref('financeur') }} as f
    on f.financeur_code = e.financeur_code
where not e.is_source_test
  and not e.is_cohorte_exclue
