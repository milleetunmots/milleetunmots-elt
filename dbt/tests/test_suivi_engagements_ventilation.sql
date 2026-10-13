-- La ventilation des trois millesimes doit reconstituer exactement le nombre
-- d'enfants suivis : si un enfant echappe aux quatre categories, ou s'il en
-- porte deux, l'engagement affiche en seance ne correspond plus a son detail.
select
    project_code,
    annee,
    nb_enfants_suivis,
    nb_enfants_annee_precedente
        + nb_enfants_annee_courante
        + nb_enfants_annee_suivante
        + nb_enfants_non_planifie as somme_ventilation
from {{ ref('suivi_engagements') }}
where nb_enfants_suivis != nb_enfants_annee_precedente
                         + nb_enfants_annee_courante
                         + nb_enfants_annee_suivante
                         + nb_enfants_non_planifie
