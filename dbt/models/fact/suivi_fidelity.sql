-- Suivi des trois indicateurs du projet Fidelity : une ligne par annee de
-- cohorte et par indicateur, avec la valeur mesuree face a la cible negociee.
--
-- La grille Fidelity ne dit pas comment ses indicateurs se calculent, elle n'en
-- donne que le libelle et une baseline 2024. Les definitions retenues ici sont
-- celles qui redonnent la baseline a la decimale pres -- c'est la seule preuve
-- disponible qu'on remesure bien la meme chose d'une annee sur l'autre :
--
--   KPI_GLOBAL  54,4 % en 2024 (grille : 54)
--   KPI_1       44,4 % en 2024 (grille : 44)
--   KPI_2       66,2 % en 2024 (grille : 66)
--
-- QUELLE METRIQUE MAISON LE KPI_GLOBAL REPREND : la [OLD], pas la [NEW]. Les
-- deux vivent dans la collection « Dashboards Certifies » de Metabase sous le
-- nom « Nouvelles habitudes - % familles avec au moins une petite mission
-- definie », partagent exactement le meme numerateur (has_set_goals, le case
-- sur les quatre is_callN_goals) et ne different que par le denominateur :
--
--   [OLD]  aucun filtre : tous les enfants de la cohorte.
--          54,4 / 60,3 / 60,9 pour 2024, 2025, 2026. C'est ce modele.
--   [NEW]  filtre is_call_0_1_status_ok : les seuls enfants joints a l'appel 0
--          ou a l'appel 1.  69,2 / 74,8 / 73,1.
--
-- Le contrat Fidelity a ete depose avec la [OLD] : c'est elle, et elle seule,
-- qui redonne la baseline 54 de la grille. Basculer sur la [NEW] ferait passer
-- toutes les annees au-dessus de leur cible -- 2026 a 73 pour une cible de 66 --
-- sans qu'aucun enfant de plus ait recu de mission. Ce serait un changement de
-- denominateur presente comme un progres.
--
-- La [NEW] n'est pas fausse pour autant : rapporter les missions aux seuls
-- enfants qu'on a joints mesure la pratique des accompagnantes plutot que la
-- couverture du programme. C'est une bonne mesure interne, ce n'est pas celle
-- du contrat. Si elle doit le devenir, le dire a Fidelity et recalculer toutes
-- les annees -- jamais la seule annee courante.
--
-- CE QUE KPI_1 MESURE VRAIMENT, et il faut le dire en seance : le champ
-- nb_of_tries_call0 n'est renseigne que lorsque l'appel a abouti -- 99,2 % des
-- appels OK de 2026 le portent, 17 % des autres statuts, 0 % des enfants sans
-- statut. Son denominateur est donc, en pratique, celui des enfants JOINTS.
-- KPI_1 repond a « combien de tentatives a-t-il fallu pour joindre celles qu'on
-- a jointes », et non a « quelle part des enfants avons-nous joints du
-- premier coup ». Consequence : il peut monter alors meme qu'on joint moins de
-- enfants. Ne jamais le presenter seul -- toujours a cote de KPI_2, qui porte
-- la couverture.
--
-- POURQUOI LA BASELINE DU KPI_1 NE PORTE QUE SUR 594 ENFANTS, et pourquoi elle
-- tient quand meme. Le champ nb_of_tries_call0 naît dans l'application en
-- octobre 2024 : zero ligne renseignee avant, 29 % en octobre, 67 % en novembre
-- et decembre. Les 594 sont donc les enfants du seul quatrieme trimestre, 10 %
-- de la cohorte 2024. Trois verifications faites le 2026-10-20 :
--   - le Q4 ressemble au reste de 2024 sur les deux indicateurs mesures sur
--     tout le monde (KPI_2 65,4 contre 66,4 ; KPI_GLOBAL 55,3 contre 54,2) ;
--   - les trois mois du Q4 s'accordent entre eux : 43,2 / 45,1 / 44,6 ;
--   - la COMPOSITION du denominateur ne derive pas, alors meme que le taux de
--     remplissage passe de 15 % a 99 % des appels OK : la part d'appels OK dans
--     le denominateur reste a 93,4 / 92,0 / 91,6 % de 2024 a 2026, et recalculer
--     le KPI sur les seuls appels OK donne la meme trajectoire (45,2 / 43,9 /
--     51,5 contre 44,4 / 42,9 / 49,5 publies).
-- Ce qui reste, et qu'il faut dire : 594 observations portent un intervalle de
-- confiance de +/- 4 points. La baisse apparente de 2024 a 2025 (44,4 -> 42,9)
-- est donc dans le bruit de la baseline elle-meme. Le retard sur la cible 2025
-- (55) est reel ; la micro-baisse ne l'est pas.
--
-- Changer une definition, c'est casser cette comparabilite : la baseline ne
-- tombera plus juste et la trajectoire presentee a Fidelity devient illisible.
-- Si une definition doit bouger, le dire explicitement au financeur et
-- recalculer toutes les annees, jamais la seule annee courante.
--
-- MAILLE : l'enfant, comme suivi_engagements et comme tout le reporting
-- financeur de ce depot. Un engagement se contractualise en nombre d'enfants a
-- accompagner, c'est donc en enfants que le financeur lit ce qu'il a finance.
-- La grille dit « parents » et « families », mais c'est le vocabulaire du
-- contrat, pas sa maille de comptage : une fratrie de deux compte pour deux.
-- Les deux mailles tombent a 0,4 point l'une de l'autre sur la baseline 2024,
-- ce qui fait qu'aucune ne se trahit par un ecart a la grille -- raison de plus
-- pour que le choix soit ecrit ici plutot que deduit d'un chiffre.
--
-- ANNEE : l'annee de demarrage de la cohorte (date_call0_start du groupe), et
-- non l'annee d'inscription ni celle de l'appel. Un indicateur suit une
-- promotion d'enfants sur tout son cycle ; une cohorte qui demarre en
-- decembre 2026 est une cohorte 2026 meme si ses appels 2 et 3 tombent en 2027.
-- Consequence directe : une annee n'est definitive que lorsque toutes ses
-- cohortes ont fini leur cycle. pct_cycle_acheve porte cette information et
-- is_annee_partielle la resume -- ne jamais presenter une annee partielle comme
-- un resultat acquis, KPI_GLOBAL remonte mecaniquement jusqu'a la cloture.
--
-- PERIMETRE : tout le programme, et non un sous-ensemble finance par Fidelity.
-- C'est ce que confirme la correspondance des trois baselines : elles ont ete
-- etablies sur l'ensemble des enfants accompagnes. Les enfants de test ne
-- sont donc pas exclues ici, contrairement a suivi_engagements -- les retirer
-- decalerait les valeurs par rapport a la grille deposee.
--
-- Les taux sont en division protegee par nullif, et non en div0null : une annee
-- sans aucune mesure doit afficher null, pas « 0 % » -- une absence de mesure
-- n'est pas un echec.

with enfants as (
    select * from {{ ref('suivi_fidelity_enfant') }}
),

-- Contexte de l'annee, identique pour ses trois indicateurs : combien de
-- enfants elle porte, et quelle part d'entre eux a deja fini son cycle.
cohortes as (
    select
        annee,
        count(*)                                                    as nb_enfants_cohorte,
        round(100.0 * avg(iff(is_cycle_acheve, 1, 0)), 1)            as pct_cycle_acheve
    from enfants
    group by annee
),

-- Les trois indicateurs remis a plat : une ligne par annee et par indicateur.
-- Le format long reproduit la grille Fidelity telle quelle une fois pivote
-- (indicateurs en lignes, annees en colonnes) et evite d'ajouter une colonne
-- au modele le jour ou un quatrieme indicateur entre au contrat.
mesures as (
    select
        annee,
        'KPI_GLOBAL'                                                as indicateur_code,
        1                                                           as indicateur_ordre,
        'Parents ayant adopté une nouvelle habitude'                as indicateur_libelle,
        'Parents who adopted a new habit in interactions with their children (%)'
                                                                    as indicateur_libelle_fidelity,
        'Enfants à qui une petite mission a été posée au moins une fois, rapportés à TOUS les enfants de la cohorte (métrique maison [OLD]) — cohortes au cycle achevé uniquement. La métrique [NEW], qui ne retient que les enfants joints, donne 69,2 % en 2024 contre 54 à la grille.'
                                                                    as indicateur_definition,
        -- La petite mission EST l'habitude nouvelle : elle est co-construite
        -- avec le parent sur une pratique concrete du quotidien. Le KPI compte
        -- donc la POSE de la mission, et s'arrete la -- ce qu'elle devient
        -- ensuite ne le regarde pas.
        --
        -- Ne pas lui substituer la mission DECLAREE REUSSIE
        -- (a_declare_une_pm_reussie, rapporte aux enfants qui en ont recu
        -- une) : cette lecture tombe elle aussi sur 54 % en 2024, ce qui la
        -- rend facile a confondre avec la bonne, mais elle diverge ensuite --
        -- 69,1 % en 2025 contre 60,3 %. Elle mesure en plus un comportement
        -- declare, et sa trajectoire est polluee par la saisie du suivi (voir
        -- la skill analyse-cohorte, section 9 bis). La correspondance sur la
        -- seule annee 2024 ne suffit donc pas a trancher entre deux
        -- definitions : c'est le metier qui tranche.
        --
        -- Le denominateur est TOUS les enfants de la cohorte, pas seulement
        -- ceux qu'on a joints. Un enfant dont la famille n'a jamais ete jointe
        -- n'a pas recu de mission : c'est un resultat, pas une donnee manquante. Rapporte aux
        -- seuls enfants joignables on lirait 56,9 % en 2024, aux seuls
        -- appels 0 aboutis 85,7 % -- ni l'un ni l'autre ne retombe sur la
        -- baseline.
        sum(a_recu_une_pm)                                          as numerateur,
        count(*)                                                    as denominateur
    from enfants
    -- Seules les cohortes dont le cycle est clos entrent dans ce KPI, et c'est
    -- ce qui le rend comparable d'une annee sur l'autre. Une mission peut etre
    -- posee a n'importe lequel des quatre appels : une cohorte qui n'a passe
    -- que l'appel 0 n'a pas un mauvais taux, elle a un taux pas encore ne.
    -- Melanger les deux fabrique une chute qui n'existe pas -- 2026 ressort a
    -- 50,0 % toutes cohortes confondues contre 60,9 % sur les seules achevees.
    -- Le prix a payer est que l'annee en cours ne couvre qu'une partie de ses
    -- cohortes : c'est pct_cycle_acheve qui le dit, et il faut le montrer a
    -- cote du chiffre.
    -- KPI_1 et KPI_2 n'ont pas besoin de ce filtre : ils se jouent entierement
    -- a l'appel 0, donc des la premiere quinzaine de la cohorte.
    where is_cycle_acheve
    group by annee

    union all

    select
        annee,
        'KPI_1',
        2,
        'Enfant joint dès la première tentative',
        'Successful first attempt to reach family (%)',
        'Appels 0 menés dès la 1ère tentative, parmi les enfants dont la tentative est renseignée — soit, en pratique, parmi les enfants joints',
        sum(jointe_des_la_1ere),
        sum(tentative_renseignee)
    from enfants
    group by annee

    union all

    select
        annee,
        'KPI_2',
        3,
        'Enfant ayant bénéficié d''un appel 0 complet',
        'Families who benefit from a first call session (%)',
        'Appels 0 au statut OK, parmi les enfants joignables (statut renseigné, hors numéro erroné et « ne pas appeler »)',
        sum(call0_ok),
        sum(call0_joignable)
    from enfants
    group by annee
),

-- Cibles de la grille Fidelity. Elles sont ecrites ici plutot que dans un seed
-- parce qu'elles sont contractuelles : cinq valeurs par indicateur, fixees pour
-- la duree du projet, qui ne bougeront qu'en cas d'avenant. Un seed ferait
-- croire qu'on peut les retoucher au fil de l'eau.
-- 2024 est la baseline et non une cible : l'ecart y vaut donc zero par
-- construction, et c'est ce qui sert de controle de non-regression du modele.
cibles as (
    select * from values
        (2024, 'KPI_GLOBAL', 54.0), (2025, 'KPI_GLOBAL', 56.5), (2026, 'KPI_GLOBAL', 66.0),
        (2027, 'KPI_GLOBAL', 72.0), (2028, 'KPI_GLOBAL', 74.0),
        (2024, 'KPI_1', 44.0), (2025, 'KPI_1', 55.0), (2026, 'KPI_1', 65.0),
        (2027, 'KPI_1', 70.0), (2028, 'KPI_1', 72.0),
        (2024, 'KPI_2', 66.0), (2025, 'KPI_2', 71.0), (2026, 'KPI_2', 77.0),
        (2027, 'KPI_2', 79.0), (2028, 'KPI_2', 80.0)
    as t (annee, indicateur_code, cible_pct)
)

select
    m.annee,
    m.indicateur_code,
    m.indicateur_ordre,
    m.indicateur_libelle,
    m.indicateur_libelle_fidelity,
    m.indicateur_definition,

    round(100.0 * m.numerateur / nullif(m.denominateur, 0), 1)      as valeur_pct,
    c.cible_pct,
    round(100.0 * m.numerateur / nullif(m.denominateur, 0) - c.cible_pct, 1) as ecart_points,
    -- L'atteinte se juge sur la valeur arrondie, celle qu'on publie, et non sur
    -- la division brute : 53,95 % affiche « 54 % » face a une cible de 54, et
    -- le lecteur n'accepterait pas de lire « en retard » a cote d'un ecart de
    -- zero point.
    case
        when c.cible_pct is null or m.denominateur = 0 then null
        when round(100.0 * m.numerateur / m.denominateur, 1) >= c.cible_pct then true
        else false
    end                                                             as is_cible_atteinte,
    -- 2024 est l'annee de reference du contrat, pas une annee a battre : la
    -- comparer a sa propre baseline n'a pas de sens, et la colonne evite que la
    -- vue d'exploitation la presente comme une performance.
    (m.annee = 2024)                                                as is_baseline,

    m.numerateur,
    m.denominateur,
    co.nb_enfants_cohorte,

    -- Part de la cohorte sur laquelle l'indicateur est reellement mesure. Basse,
    -- elle signale que le taux repose sur une poignee d'enfants et ne se
    -- compare pas aux autres annees : c'est le cas de KPI_1 en 2024 (10 %).
    round(100.0 * m.denominateur / nullif(co.nb_enfants_cohorte, 0), 1) as taux_couverture_mesure,

    co.pct_cycle_acheve,
    -- Une annee dont toutes les cohortes n'ont pas fini leur cycle est encore
    -- en mouvement : les petites missions des appels 2 et 3 ne sont pas toutes
    -- declarees, KPI_GLOBAL y est donc sous-estime.
    (co.pct_cycle_acheve < 100)                                     as is_annee_partielle

from mesures as m
left join cibles as c
    on c.annee = m.annee
    and c.indicateur_code = m.indicateur_code
left join cohortes as co
    on co.annee = m.annee
