{{ config(materialized='view') }}

-- Le detail du suivi des engagements, rattache aux financeurs TELS QUE LE
-- METIER LES SUIT : une ligne par enfant et par annee de suivi, celles de
-- suivi_engagements_enfant, plus le financeur et la mesure « inscrit sur
-- l'annee ». Regles arretees avec le metier le 2026-11-10.
--
-- POURQUOI UN SECOND RATTACHEMENT. suivi_engagements_enfant porte deja un
-- project_code, derive de source_project (canal x departement de la source).
-- Le metier compte autrement : trois departements se lisent Caf et CD
-- confondus (10, 60, 63), le CD 92 se rattache aussi par le departement de la
-- FAMILLE, le CD 91 ne prend que le canal PMI. La regle du 92 depend de
-- l'enfant et non de sa source : elle ne peut pas vivre dans source_project.
-- Les deux rattachements cohabitent tant que le dashboard « Suivi de projet »
-- d'origine existe ; a terme, celui-ci remplace l'autre.
--
-- LES REGLES SONT CELLES DES FILTRES DU DASHBOARD MECENES ET PARTENAIRES
-- (« Departement de la source », « Canal d'inscription », « Source precise »,
-- « Departement famille ou source »), que le metier utilise aujourd'hui a la
-- main, financeur par financeur. Controle du 2026-11-10 : les 24 regles
-- redonnent, pour les inscrits 2026, exactement les comptes de fact.mecene
-- (5 929 au total) -- a une exception pres, voulue, ci-dessous.
--
-- UN ENFANT = UN SEUL FINANCEUR. Les filtres du metier sont independants : un
-- enfant domicilie dans le 92 et inscrit par la Caf 93 y compte deux fois, une
-- par filtre. Ici le case s'arrete a la premiere regle qui matche, et la
-- clause « famille dans le 92 » passe APRES les regles de source : elle ne
-- capte que les enfants dont la source ne releve d'aucun autre financeur de la
-- liste (bouche-a-oreille, petits partenaires). Le CD 92 affiche donc 352
-- inscrits en 2026 la ou le filtre de mecene en donne 358 : les 6 d'ecart sont
-- comptes chez le financeur qui les a inscrits.
--
-- LES SOURCES NOMMEES SONT DESIGNEES PAR LEUR IDENTIFIANT, pas par leur nom :
-- un renommage dans l'application ne doit pas vider une ligne du tableau. Le
-- nom est en commentaire. Listes strictement celles du metier : la creche Les
-- Petits Hochets (78), « PMI Paris 18e - Tchaikowski » (158), l'ancienne « PMI
-- Paris 18e » (92) et le doublon archive « TS CAF de la Marne » (361), que
-- source_project rattachait a la Caf 93, a la Cite educative et a la Caf 51,
-- tombent dans AUTRES. 4 enfants inscrits a elles quatre, tous en 2025.
--
-- AUTRES EST PROVISOIRE (« a definir ensemble », metier, 2026-11-10) : tout ce
-- qui ne matche aucune regle, hors bouche-a-oreille et sources de test. On y
-- trouve donc les petits partenaires, mais aussi les anciens financeurs que le
-- tableau ne liste plus -- CD 69 et CD 78 y pesent 232 des 415 reports de 2026.
--
-- LA MESURE « INSCRIT SUR L'ANNEE » est celle de mecene : enfant cree dans
-- l'annee, statut different de not_supported (le filtre
-- « Enfant_accompagne_seulement » du dashboard Mecenes, vrai par defaut).
-- Ce n'est PAS is_entree_dans_annee, qui date l'enfant par le demarrage de sa
-- cohorte : un enfant inscrit en decembre 2025 et entre en cohorte en janvier
-- 2026 est un inscrit de 2025 et une entree de 2026.
--
-- Vue, et non table : elle n'ajoute que deux colonnes calculees a
-- suivi_engagements_enfant et doit en suivre chaque reconstruction.
--
-- Rien n'est filtre ici, comme dans suivi_engagements_enfant : les cohortes
-- exclues et les sources de test restent visibles, pour l'audit. Le perimetre
-- du dashboard est pose une fois, dans analytics.engagements_enfants_par_financeur.

with enfants as (
    select
        *,
        -- l'application stocke le departement sans zero initial ('1' pour l'Ain)
        lpad(source_departement, 2, '0') as source_dep
    from {{ ref('suivi_engagements_enfant') }}
),

rattaches as (
    select
        enfants.*,
        case
            -- hors perimetre d'abord : une source de test ne compte pour
            -- personne, meme si la famille habite le 92
            when is_source_test                                     then 'TEST'

            when source_dep = '93' and source_channel = 'pmi'       then 'CD_93'
            when source_id = '3'   /* CAF 93 */                     then 'CAF_93'
            when source_dep = '01' and source_channel = 'pmi'       then 'CD_01'
            when source_id = '220' /* CAF Ain */                    then 'CAF_01'
            -- Caf et CD confondus : toutes les sources du departement,
            -- partenaires locaux compris
            when source_dep = '10'                                  then 'DEP_10'
            when source_dep = '27' and source_channel = 'pmi'       then 'CD_27'
            when source_dep = '44' and source_channel = 'pmi'       then 'CD_44'
            when source_dep = '45' and source_channel = 'pmi'       then 'CD_45'
            when source_id = '350' /* [51] TS - CAF de la Marne */  then 'CAF_51'
            when source_id = '164' /* [51] Cite educative Reims Secteur SUD */
                                                                    then 'CE_REIMS'
            when source_dep = '55' and source_channel = 'pmi'       then 'CD_55'
            when source_dep = '57' and source_channel = 'pmi'       then 'CD_57'
            when source_id = '4'   /* CAF Moselle */                then 'CAF_57'
            when source_dep = '60'                                  then 'DEP_60'
            when source_dep = '63'                                  then 'DEP_63'
            when source_id = '162' /* [69] Ville de Lyon - Creches municipales */
                                                                    then 'MAIRIE_LYON'
            -- [75] Cite educative - Paris 18e, Creche Huchard, Creche
            -- Richomme, PMI Paris 18e - Binet, Cave, Ney
            when source_id in ('73', '339', '341', '161', '160', '159')
                                                                    then 'CE_PARIS_18'
            when source_dep = '77' and source_channel = 'pmi'       then 'CD_77'
            when source_id = '228' /* CAF Seine-et-Marne */         then 'CAF_77'
            when source_dep = '83' and source_channel = 'pmi'       then 'CD_83'
            when source_id = '314' /* CAF Var */                    then 'CAF_83'
            when source_id = '151' /* CAF Essonne */                then 'CAF_91'
            -- le seul canal PMI : les AEMO de l'Essonne, que source_project
            -- rattachait au CD 91, tombent dans AUTRES (5 inscrits en 2026)
            when source_dep = '91' and source_channel = 'pmi'       then 'CD_91'
            -- source OU famille dans le 92 ; apres toutes les regles de source,
            -- pour ne retirer aucun enfant au financeur qui l'a inscrit
            when source_dep = '92' or famille_departement = '92'    then 'CD_92'

            -- ce que personne ne finance
            when source_channel in ('bao', 'other')                 then 'BAO'
            when source_id is null                                  then 'SANS_SOURCE'
            else 'AUTRES'
        end as financeur_code
    from enfants
)

select
    rattaches.* exclude (source_dep),

    -- La mesure opposee a l'engagement du partenaire. coalesce : un statut
    -- vide ne compte pas, comme dans mecene ou is_accompanied vaut alors null.
    is_inscrit_dans_annee
        and coalesce(group_status != 'not_supported', false) as is_inscrit_compte_dans_annee
from rattaches
