-- La portee ventilee par millesime doit retomber sur celle du suivi par projet.
-- C'est le garde-fou de la jointure croisee de engagements_par_millesime : si
-- elle se dedouble, un enfant est compte deux fois et le total s'envole sans
-- que rien ne le signale.
with par_millesime as (
    select annee, project_code, sum(nb_enfants) as nb_enfants
    from {{ ref('engagements_par_millesime') }}
    group by annee, project_code
),

par_projet as (
    select annee, project_code, nb_enfants_suivis
    from {{ ref('suivi_engagements') }}
)

select
    p.annee,
    p.project_code,
    p.nb_enfants_suivis,
    coalesce(m.nb_enfants, 0) as nb_enfants_ventiles
from par_projet as p
left join par_millesime as m
    on m.annee = p.annee
   and m.project_code = p.project_code
where p.nb_enfants_suivis != coalesce(m.nb_enfants, 0)
