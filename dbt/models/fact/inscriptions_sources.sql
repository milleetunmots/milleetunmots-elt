{{ config(materialized='table') }}

-- Les inscriptions par source : une ligne par enfant inscrit via une source.
-- C'est la definition d'une inscription pour tout le suivi de l'activite des
-- sources -- fact.activite_sources en tire ses statuts, et
-- analytics.inscriptions_sources_mois sa courbe. Un seul endroit, pour que le
-- graphique d'une source retombe toujours sur les chiffres de son statut.
--
-- Perimetre : hors reinscriptions, hors enfants supprimes, hors sources de test
-- et bouche-a-oreille (project_code TEST / BAO de dim.source_project).

with source_project as (
    select * from {{ ref('source_project') }}
),

children_sources as (
    select * from {{ ref('stg_1001mots_app__children_sources') }}
),

children as (
    select * from {{ ref('stg_1001mots_app__children') }}
)

select
    cs.source_id,
    sp.source_name,
    sp.project_label,
    cs.child_id,
    cs.date_created as date_inscription,
    cs.professional_email
from children_sources as cs
inner join children as c
    on c.child_id = cs.child_id
inner join source_project as sp
    on sp.source_id = cs.source_id
where not coalesce(cs.is_reenrollment, false)
    and c.date_discarded is null
    and sp.project_code not in ('TEST', 'BAO')
