-- Invariant de comparabilite : la baseline 2024 doit redonner la grille deposee
-- chez Fidelity, a 0,5 point pres.
--
-- La grille ne porte pas les definitions de ses indicateurs : elle n'en donne
-- que le libelle anglais et trois valeurs pour 2024. Les definitions du modele
-- ont ete retrouvees en cherchant ce qui redonne exactement ces trois valeurs.
-- Cette correspondance est donc la SEULE preuve disponible qu'on remesure bien
-- la meme chose d'une annee sur l'autre, et c'est ce que ce test protege : si
-- une definition derive -- un denominateur elargi, un statut d'appel ajoute, la
-- maille qui repasse a l'enfant -- la baseline cesse de tomber juste et la
-- trajectoire presentee au financeur devient une comparaison de deux choses
-- differentes, sans que rien d'autre ne le signale.
--
-- La tolerance de 0,5 point couvre l'arrondi de la grille, qui est a l'entier.
-- Elle ne couvre rien d'autre : un ecart de 2 points est une derive, pas un
-- arrondi.
--
-- Si ce test tombe, NE PAS l'ajuster pour le faire passer. Soit la definition a
-- change par accident et il faut la retablir, soit elle a change volontairement
-- et il faut alors le dire a Fidelity, recalculer TOUTES les annees et mettre a
-- jour les valeurs attendues ici.

with attendu as (
    select * from values
        ('KPI_GLOBAL', 54.0),
        ('KPI_1',      44.0),
        ('KPI_2',      66.0)
    as t (indicateur_code, valeur_grille)
),

mesure as (
    select indicateur_code, valeur_pct
    from {{ ref('suivi_fidelity') }}
    where annee = 2024
)

select
    a.indicateur_code,
    a.valeur_grille,
    m.valeur_pct                                as valeur_mesuree,
    round(m.valeur_pct - a.valeur_grille, 1)    as ecart_points
from attendu as a
left join mesure as m
    on m.indicateur_code = a.indicateur_code
-- l'indicateur a disparu du modele, ou sa valeur s'est eloignee de la grille
where m.indicateur_code is null
   or m.valeur_pct is null
   or abs(m.valeur_pct - a.valeur_grille) > 0.5
