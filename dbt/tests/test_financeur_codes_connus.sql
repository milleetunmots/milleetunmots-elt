-- Tout code que le case de suivi_financeurs_enfant produit doit exister dans
-- dim.financeur, hors les trois sorties de perimetre. La vue du dashboard fait
-- une jointure interne sur le referentiel : un code mal orthographie d'un cote
-- ou de l'autre ferait disparaitre ses enfants sans laisser de trace.
select
    e.financeur_code,
    count(*) as nb_lignes
from {{ ref('suivi_financeurs_enfant') }} as e
left join {{ ref('financeur') }} as f
    on f.financeur_code = e.financeur_code
where f.financeur_code is null
  and e.financeur_code not in ('TEST', 'BAO', 'SANS_SOURCE')
group by e.financeur_code
