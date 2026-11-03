{{ config(materialized='view') }}

-- Vue d'exploitation de l'alerte « sources a relancer » : les seules sources qui
-- appellent un email, avec de quoi l'ecrire sans autre recherche.
--
-- De gauche a droite :
--   1. le verdict   statut, et depuis quand
--   2. qui          la source, son financeur, a qui ecrire
--   3. le pourquoi  rythme habituel, volume recent, volume un an plus tot
--   4. l'habitude   anciennete, plus long silence passe, mois forts : de quoi
--                   dire si le silence actuel sort de ses habitudes
--
-- Trois statuts, du plus urgent au moins urgent : inactive (signal backteste),
-- en_baisse, jamais_demarree. Les dormantes (plus d'un an de silence) n'y sont
-- pas : une relance n'y est plus une alerte mais une reprise de partenariat.
--
-- Vue, et non table : les regles vivent dans fact.activite_sources.
--
-- PMI et partenaires locaux seulement (demande du 2026-11-03) : les CAF sont
-- hors du dashboard. fact.activite_sources garde toutes les sources.

select
    -- ---------------------------------------------------------------- 1. verdict
    statut,
    date_bascule_inactive,
    jours_sans_inscription,
    -- ------------------------------------------------------------------- 2. qui
    source_id,
    source_name                 as source,
    channel                     as canal,
    departement,
    project_label               as financeur,
    email_contact_principal,
    emails_contacts,
    nb_contacts,
    -- ------------------------------------------------------------- 3. pourquoi
    date_derniere_inscription,
    intervalle_habituel_jours,
    seuil_silence_jours,
    nb_jours_actifs_365j,
    nb_inscriptions_365j,
    nb_inscriptions_90j,
    nb_inscriptions_90j_n1,
    nb_inscriptions_180j,
    nb_inscriptions_180j_n1,
    -- --------------------------------------------------------------- 4. habitude
    date_creation_source,
    date_premiere_inscription,
    nb_inscriptions_total,
    round(nb_inscriptions_par_mois_historique, 1) as nb_inscriptions_par_mois_historique,
    plus_long_silence_passe_jours,
    jours_sans_inscription > plus_long_silence_passe_jours as is_silence_record,
    nb_inscriptions_meme_periode_n1,
    mois_les_plus_actifs
from {{ ref('activite_sources') }}
where statut in ('inactive', 'en_baisse', 'jamais_demarree')
    and channel in ('pmi', 'local_partner')
