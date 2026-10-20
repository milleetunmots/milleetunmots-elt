-- Invariant de couverture : tout enfant non supprime de l'application doit
-- apparaitre au moins une fois dans le suivi, sur au moins une annee. Un enfant
-- absent n'est compte pour personne et ne laisse aucune trace -- c'est
-- exactement le genre de perte qu'un total ne signale pas.
--
-- Trois chemins le garantissent : la categorie B pour qui entre en cohorte, la
-- categorie E pour qui est inscrit sans jamais etre accompagne, et le code
-- 'SANS_SOURCE' pour qui n'a pas de source renseignee.
with fait as (
    select distinct child_id from {{ ref('suivi_engagements_enfant') }}
)

select
    c.child_id,
    c.date_created,
    c.group_id
from {{ ref('child') }} as c
where c.date_discarded is null
  and not exists (select 1 from fait f where f.child_id = c.child_id)
