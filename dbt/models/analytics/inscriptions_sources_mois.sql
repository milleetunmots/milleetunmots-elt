{{ config(materialized='view') }}

-- Inscriptions mensuelles de chaque source, pour tracer son activite dans le
-- temps a cote de son statut d'alerte. Une ligne par source et par mois, de son
-- premier mois d'inscription au mois en cours, les mois sans inscription a 0 :
-- un trou dans la courbe doit se voir comme un trou, pas disparaitre.
--
-- Porte aussi le meme mois un an plus tot, pour lire la saisonnalite propre a
-- la source sans calcul cote Metabase.
--
-- Tout l'historique est la ; la fenetre (annee en cours) est posee par la
-- carte. La grille partant de la premiere inscription, une source plus recente
-- s'affiche naturellement depuis son debut.
--
-- Toutes les PMI et partenaires locaux y sont, pas seulement ceux a relancer :
-- on doit pouvoir regarder n'importe quelle source. Les CAF en sont retirees,
-- comme de analytics.sources_a_relancer, pour que la courbe et les listes
-- portent sur les memes sources. Le mois en cours est partiel.

with inscriptions as (
    select * from {{ ref('inscriptions_sources') }}
),

-- le statut d'alerte du jour, pour que le filtre Statut du dashboard porte
-- aussi sur la courbe
activite as (
    select source_id, statut, channel from {{ ref('activite_sources') }}
),

sources as (
    select
        source_id,
        source_name,
        project_label,
        date_trunc(month, min(date_inscription))::date as premier_mois
    from inscriptions
    where source_id in (select source_id from activite
                        where channel in ('pmi', 'local_partner'))
    group by source_id, source_name, project_label
),

mois as (
    select distinct date_trunc(month, full_date)::date as mois
    from {{ ref('calendar') }}
    where full_date <= current_date
),

grille as (
    select s.source_id, s.source_name, s.project_label, s.premier_mois, m.mois
    from sources as s
    inner join mois as m
        on m.mois >= s.premier_mois
),

par_mois as (
    select source_id, date_trunc(month, date_inscription)::date as mois, count(*) as nb
    from inscriptions
    group by source_id, date_trunc(month, date_inscription)::date
)

select
    g.source_name                          as source,
    g.project_label                        as financeur,
    g.source_id,
    a.statut,
    g.mois,
    coalesce(p.nb, 0)                      as nb_inscriptions,
    -- null avant le premier mois de la source : pas d'historique, pas zero
    iff(dateadd(year, -1, g.mois) >= g.premier_mois,
        coalesce(p_n1.nb, 0), null)        as nb_inscriptions_n1,
    g.mois = date_trunc(month, current_date) as is_mois_en_cours
from grille as g
left join par_mois as p
    on p.source_id = g.source_id
    and p.mois = g.mois
left join activite as a
    on a.source_id = g.source_id
left join par_mois as p_n1
    on p_n1.source_id = g.source_id
    and p_n1.mois = dateadd(year, -1, g.mois)
