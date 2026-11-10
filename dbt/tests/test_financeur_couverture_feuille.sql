-- Tout engagement non nul de la feuille du bureau doit etre repris par un
-- financeur du referentiel dim.financeur. Sans quoi il sort du tableau et du
-- total du dashboard sans rien signaler : c'est ce qui arriverait le jour ou
-- le bureau ajoute un partenaire a la feuille, ou renomme une ligne au point
-- que stg_projects__engagements n'en derive plus de code.
--
-- Les lignes a engagement nul ou vide (ARS, anciens partenaires) sont
-- volontairement hors referentiel et ne font pas echouer le test.
select
    e.annee,
    e.project_label,
    e.project_code,
    e.nb_enfants_engages
from {{ ref('stg_projects__engagements') }} as e
left join {{ ref('financeur') }} as f
    on array_contains(e.project_code::variant, f.codes_feuille)
where f.financeur_code is null
  and coalesce(e.nb_enfants_engages, 0) != 0
