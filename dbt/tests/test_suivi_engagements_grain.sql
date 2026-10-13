-- Une seule ligne par projet et par annee : sans ca, un financeur apparaitrait
-- deux fois dans le dashboard du bureau avec deux engagements.
select
    project_code,
    annee,
    count(*) as nb_lignes
from {{ ref('suivi_engagements') }}
group by project_code, annee
having count(*) > 1
