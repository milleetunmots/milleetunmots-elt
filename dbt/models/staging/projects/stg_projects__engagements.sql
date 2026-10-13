-- Engagements et projections du bureau, une ligne par financeur et par annee.
-- Les deux mesures sont des NOMBRES D'ENFANTS a inscrire sur l'annee civile.
--
-- Le libelle saisi dans la feuille (colonne par_dpartements) est le seul
-- identifiant disponible. On en derive un code stable (project_code) qui sert
-- de cle de jointure avec les sources d'inscription de l'application
-- (voir source_project). Le code colle le type de financeur et le departement
-- sur deux chiffres : 'CD_93', 'CAF_01'. Les financeurs qui ne sont pas
-- departementaux ont un code en dur.

with source_2026 as (
    select
        2026 as annee,
        trim(par_dpartements) as project_label,
        engagement::integer   as nb_enfants_engages,
        projection::integer   as nb_enfants_projetes
    from {{ source('projects', 'export_data_2026') }}
    where nullif(trim(par_dpartements), '') is not null
),

-- Une nouvelle annee s'ajoute ici en union all.
unioned as (
    select * from source_2026
),

coded as (
    select
        annee,
        project_label,
        -- La PMI est un service du Conseil Departemental : les lignes « PMI xx »
        -- de la feuille (44, 60) partagent le code 'CD_xx' avec les lignes
        -- « CD xx », elles ne coexistent jamais pour un meme departement.
        case
            when upper(project_label) like 'CD %'            then 'CD_'  || lpad(regexp_substr(project_label, '[0-9]+'), 2, '0')
            when upper(project_label) like 'PMI %'           then 'CD_'  || lpad(regexp_substr(project_label, '[0-9]+'), 2, '0')
            when upper(project_label) like 'CAF %'           then 'CAF_' || lpad(regexp_substr(project_label, '[0-9]+'), 2, '0')
            when upper(project_label) like 'ARS %'           then 'ARS_' || lpad(regexp_substr(project_label, '[0-9]+'), 2, '0')
            when upper(project_label) like 'MAIRIE DE LYON%' then 'MAIRIE_LYON'
            when upper(project_label) like '%REIMS%'         then 'CE_REIMS'
            when upper(project_label) like '%18%'            then 'CE_PARIS_18'
            when upper(project_label) like 'AUTRES%'         then 'AUTRES'
        end as project_code,
        case
            when upper(project_label) like 'CD %'  then 'conseil_departemental'
            when upper(project_label) like 'PMI %' then 'conseil_departemental'
            when upper(project_label) like 'CAF %' then 'caf'
            when upper(project_label) like 'ARS %' then 'ars'
            else 'autre'
        end as financeur_type,
        case
            when upper(project_label) like 'CD %'
              or upper(project_label) like 'PMI %'
              or upper(project_label) like 'CAF %'
              or upper(project_label) like 'ARS %'
            then lpad(regexp_substr(project_label, '[0-9]+'), 2, '0')
        end as departement,
        nb_enfants_engages,
        nb_enfants_projetes
    from unioned
)

select * from coded
