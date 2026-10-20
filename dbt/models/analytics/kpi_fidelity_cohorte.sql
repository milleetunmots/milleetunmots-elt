{{ config(materialized='view') }}

-- La dynamique du suivi Fidelity : une ligne par cohorte, les trois indicateurs
-- du contrat en colonnes, plus l'entonnoir du dispositif de prise de RDV.
--
-- C'est la vue a mettre sous la grille annuelle. L'annee ne donne que trois
-- points et lisse tout ce qui bouge : sur 2026, KPI_1 passe de 34 % sur les
-- cohortes d'avril a 57 % sur celles de septembre, ce que le 49,4 % annuel ne
-- montre pas. Une cohorte, c'est une quinzaine de jours d'appels : c'est la
-- bonne maille pour voir un dispositif monter en charge.
--
-- De gauche a droite :
--   1. le repere      la cohorte, sa date, son volume
--   2. les KPI        les trois indicateurs du contrat, memes definitions
--   3. le dispositif  proposition, transformation, RDV pris
--   4. la maturite    ce qui n'est pas encore mesurable
--
-- Les taux sont des ratios de sommes et non des moyennes de taux : les cohortes
-- vont de 15 a 560 enfants, les moyenner a poids egal fausserait la lecture.
--
-- LECTURE DU DISPOSITIF -- deux leviers, pas un :
--   pct_rdv_propose        part des enfants dont la famille a recu le lien
--                          envoye. C'est une pratique d'accompagnante, en tout
--                          ou rien : dans une cohorte donnee la plupart sont a
--                          0 % ou a 100 %. C'est le levier actionnable.
--   pct_transformation     part des enfants sollicites dont la famille a effectivement
--                          pose un creneau. C'est la decision de la famille.
--   pct_rdv_pris           le produit des deux, le resultat.
-- Savoir lequel des deux bloque est tout l'interet de les separer : fin 2026 la
-- proposition plafonne a 85-95 % et la transformation a 45-50 %, donc le
-- gisement est desormais du cote des familles, plus des accompagnantes.
--
-- CE QU'IL NE FAUT PAS FAIRE AVEC CES COLONNES. Ne pas calculer le KPI_2 des
-- enfants « sans RDV » pour le comparer a celui des enfants « avec RDV » :
-- l'ecart brut (80-90 % contre 55-60 %) n'est pas l'effet du dispositif, les
-- familles qui prennent RDV sont deja celles qui repondent. Quand l'adoption
-- monte, le groupe sans RDV devient un residu de plus en plus negativement
-- selectionne et sa baisse est un artefact. L'effet reel, mesure en comparant
-- les accompagnantes adoptantes aux non-adoptantes a projection egale, est
-- d'environ +7 points -- voir la skill analyse-cohorte, section 9.
--
-- Vue, et non table : elle ne fait qu'agreger fact.suivi_fidelity_enfant, ou
-- vivent les definitions.

with enfants as (
    select * from {{ ref('suivi_fidelity_enfant') }}
)

select
    -- ----------------------------------------------------------------- 1. repere
    cohort_name                                                         as cohorte,
    annee,
    date_call0_start                                                    as date_debut_appels,
    count(*)                                                            as enfants,

    -- -------------------------------------------------------------------- 2. KPI
    -- Part des enfants a qui une petite mission a ete posee au moins une fois.
    -- La mission est l'habitude nouvelle : le KPI compte sa pose, pas ce qu'elle
    -- devient ensuite.
    -- Null tant que le cycle n'est pas clos, et c'est delibere : une mission
    -- peut etre posee a n'importe lequel des quatre appels, donc une cohorte qui
    -- vient de passer son appel 0 affiche une valeur pas encore nee. Tracee
    -- telle quelle, elle dessine un effondrement qui n'existe pas. Le compte
    -- brut reste lisible dans enfants_avec_une_pm.
    case when booland_agg(is_cycle_acheve)
         then round(100.0 * sum(a_recu_une_pm) / nullif(count(*), 0), 1)
    end                                                                 as kpi_global_nouvelle_habitude,
    round(100.0 * sum(jointe_des_la_1ere)
                / nullif(sum(tentative_renseignee), 0), 1)              as kpi1_jointe_1ere_tentative,
    round(100.0 * sum(call0_ok)
                / nullif(sum(call0_joignable), 0), 1)                   as kpi2_appel0_complet,

    -- ------------------------------------------------------------- 3. dispositif
    round(100.0 * sum(rdv_propose) / nullif(count(*), 0), 1)            as pct_rdv_propose,
    round(100.0 * sum(rdv_propose * rdv_pris)
                / nullif(sum(rdv_propose), 0), 1)                       as pct_transformation,
    round(100.0 * sum(rdv_pris) / nullif(count(*), 0), 1)               as pct_rdv_pris,
    -- 22 enfants de 2026 ont pris un RDV sans proposition detectee (1,8 % des
    -- RDV) : la detection de la proposition passe par l'utm du SMS et en rate
    -- quelques-uns. D'ou pct_rdv_pris legerement superieur au produit des deux
    -- autres colonnes -- l'ecart est le compte de ces cas.
    sum(iff(rdv_pris = 1 and rdv_propose = 0, 1, 0))                    as rdv_sans_proposition,

    -- --------------------------------------------------------------- 4. maturite
    -- Une cohorte dont la fenetre d'appel 0 n'est pas close n'a pas de KPI_1 ni
    -- de KPI_2 : filtrer dessus avant de tracer la courbe, sinon les cohortes a
    -- venir plongent a zero.
    booland_agg(is_call0_termine)                                          as appels_0_termines,
    -- KPI_GLOBAL demande le cycle entier : les petites missions des appels 2 et
    -- 3 ne sont declarees qu'a la fin. Une cohorte recente le sous-estime.
    booland_agg(is_cycle_acheve)                                           as cycle_termine,

    -- NUMERATEURS ET DENOMINATEURS BRUTS. Ils sont ici pour qu'un consommateur
    -- puisse REPOOLER a une autre maille que la cohorte -- par mois, par
    -- trimestre, ou simplement par date de demarrage quand deux cohortes
    -- partagent la meme. Un taux ne s'additionne jamais : il faut sommer les
    -- numerateurs et les denominateurs, puis diviser.
    -- Le cas s'est produit : « 2025/03/24 - Mars25-B » et
    -- « 2025/03/24 - Mars25-B-souscription » demarrent le meme jour, et un
    -- graphique ayant la date en dimension affichait 154 % -- la somme de 73,7
    -- et 80,3.
    sum(a_recu_une_pm)                                                  as enfants_avec_une_pm,
    sum(tentative_renseignee)                                           as enfants_joints,
    sum(jointe_des_la_1ere)                                             as enfants_joints_1ere_tentative,
    sum(call0_joignable)                                                as enfants_joignables,
    sum(call0_ok)                                                       as enfants_call0_ok,
    sum(rdv_propose)                                                    as enfants_rdv_propose,
    sum(rdv_propose * rdv_pris)                                         as enfants_rdv_propose_et_pris,
    sum(rdv_pris)                                                       as enfants_rdv_pris

from enfants
group by cohort_name, annee, date_call0_start
