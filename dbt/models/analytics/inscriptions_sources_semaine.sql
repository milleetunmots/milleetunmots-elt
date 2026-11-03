{{ config(materialized='view') }}

-- Inscriptions hebdomadaires de chaque source, pour tracer son activite dans le
-- temps a cote de son statut d'alerte. Une ligne par source et par semaine
-- (lundi), de sa premiere semaine d'inscription a la semaine en cours, les
-- semaines sans inscription a 0 : un trou dans la courbe doit se voir comme un
-- trou, pas disparaitre.
--
-- A la semaine plutot qu'au mois (demande du 2026-11-03) : un silence se compte
-- en semaines, et un mois agrege masque une source qui s'est tue le 5.
--
-- Porte aussi la meme semaine un an plus tot (52 semaines, donc meme jour de la
-- semaine), pour lire la saisonnalite propre a la source sans calcul cote
-- Metabase.
--
-- Tout l'historique est la ; la fenetre de 15 mois est posee par la carte. La
-- grille partant de la premiere inscription, une source plus recente s'affiche
-- naturellement depuis son debut.
--
-- Toutes les sources du perimetre y sont, pas seulement celles a relancer : on
-- doit pouvoir regarder n'importe quelle source. La semaine en cours est
-- partielle.

with inscriptions as (
    select * from {{ ref('inscriptions_sources') }}
),

-- le statut d'alerte du jour, pour que le filtre Statut du dashboard porte
-- aussi sur la courbe
activite as (
    select source_id, statut from {{ ref('activite_sources') }}
),

sources as (
    select
        source_id,
        source_name,
        project_label,
        date_trunc(week, min(date_inscription)) as premiere_semaine
    from inscriptions
    group by source_id, source_name, project_label
),

semaines as (
    select distinct date_trunc(week, full_date) as semaine
    from {{ ref('calendar') }}
    where full_date <= current_date
),

grille as (
    select s.source_id, s.source_name, s.project_label, s.premiere_semaine, w.semaine
    from sources as s
    inner join semaines as w
        on w.semaine >= s.premiere_semaine
),

par_semaine as (
    select source_id, date_trunc(week, date_inscription) as semaine, count(*) as nb
    from inscriptions
    group by source_id, date_trunc(week, date_inscription)
)

select
    g.source_name                          as source,
    g.project_label                        as financeur,
    g.source_id,
    a.statut,
    g.semaine,
    coalesce(p.nb, 0)                      as nb_inscriptions,
    -- null avant la premiere semaine de la source : pas d'historique, pas zero
    iff(dateadd(week, -52, g.semaine) >= g.premiere_semaine,
        coalesce(p_n1.nb, 0), null)        as nb_inscriptions_n1,
    g.semaine = date_trunc(week, current_date) as is_semaine_en_cours
from grille as g
left join par_semaine as p
    on p.source_id = g.source_id
    and p.semaine = g.semaine
left join activite as a
    on a.source_id = g.source_id
left join par_semaine as p_n1
    on p_n1.source_id = g.source_id
    and p_n1.semaine = dateadd(week, -52, g.semaine)
