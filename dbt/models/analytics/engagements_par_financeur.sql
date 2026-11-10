{{ config(materialized='view') }}

-- Le tableau du dashboard « Suivi de projet v2 » : une ligne par financeur du
-- referentiel dim.financeur et par annee. Ce que le partenaire s'est engage a
-- inscrire, lu dans la feuille du bureau, face aux enfants inscrits sur l'annee.
--
-- LA MESURE OPPOSEE A L'ENGAGEMENT EST LE NOMBRE D'ENFANTS INSCRITS SUR
-- L'ANNEE (demande du metier, 2026-11-10), et non plus les entrees en cohorte
-- de engagements_par_projet. L'engagement de la feuille est un nombre d'enfants
-- a inscrire : on lui oppose des inscriptions. Definition et rattachement dans
-- fact.suivi_financeurs_enfant.
--
-- Deux colonnes viennent de la feuille et se corrigent dans la feuille :
-- statut et engagement_partenaire. Un financeur qui regroupe deux lignes de la
-- feuille (Aube, Oise, Puy-de-Dome) en additionne les engagements ;
-- lignes_feuille dit lesquelles.
--
-- LA PROJECTION EST CALCULEE ICI, PLUS LUE DANS LA FEUILLE (demande du metier,
-- 2026-11-10, « au moins pour l'instant ») : les inscrits a ce jour, prolonges
-- jusqu'au 31 decembre au rythme des 56 derniers jours. La valeur saisie par le
-- bureau reste disponible dans projection_feuille, pour y revenir.
-- Pourquoi 56 jours : rejouee un 10 novembre sur 2023, 2024 et 2025, la regle
-- se trompe de 2 %, 3 % et 10 % (somme des ecarts par financeur rapportee au
-- reel), contre 8 a 10 % pour un prorata lineaire de l'annee et 7 a 14 % pour
-- le profil saisonnier de l'annee precedente. ELLE VAUT POUR L'AUTOMNE : un
-- 10 septembre elle se trompe de 11 a 22 %, un 10 juin de 29 a 92 % -- les
-- inscriptions arrivent par vagues, et aucune methode ne fait mieux avant
-- l'ete. Avant le 28 janvier elle est vide ; une annee close, elle vaut le
-- realise.
-- « Ce jour » est le dernier jour d'inscription charge dans l'entrepot, pas
-- current_date : le week-end, les tables s'arretent au dernier run.
--
-- LE STATUT N'EXISTE PAS ENCORE DANS LA FEUILLE (2026-11-10). Il est lu par
-- object_construct(*), qui rend null tant que la colonne manque au lieu de
-- faire echouer la vue : le jour ou la feuille porte une colonne « Statut » et
-- ou la synchronisation l'a amenee, il s'affiche sans toucher au code. A
-- ramener alors dans stg_projects__engagements, ou vit le reste de la feuille
-- -- et ou une nouvelle annee s'ajoute, ici comme la-bas.
--
-- places_restantes est plancher a zero : un financeur qui a depasse son
-- engagement n'a plus de place a pourvoir, il n'en rend pas aux autres. C'est
-- pourquoi la somme des places restantes n'est PAS l'engagement total moins
-- les inscrits totaux -- la Caf 93 a elle seule depasse le sien de 849.
--
-- UN INSCRIT EST UN ENFANT AJOUTE A LA BASE DANS L'ANNEE, qu'il soit ou non
-- entre en cohorte (definition rappelee par le metier, 2026-11-10) : c'est
-- enfants_inscrits, date sur la creation de l'enfant. Les entrees en cohorte
-- et les reports sont une autre lecture, gardee a cote.
-- enfants_inscrits_ou_accompagnes est la somme de ces deux-la, comme le metier
-- l'a demandee. Elle ne contient donc pas les inscrits de l'annee dont aucune
-- cohorte ne tourne dans l'annee (138 en 2026), que la tuile du meme nom du
-- dashboard Mecenes et Partenaires ajoute.
--
-- Les colonnes d'engagement sont null, et non 0, pour les annees que la
-- feuille ne porte pas : pas d'objectif, pas de retard.
--
-- Aucune annee n'est filtree ici. Ne jamais sommer les annees sur les reports :
-- un enfant a cheval sur deux annees est une entree de la premiere et un
-- report de la seconde.

with financeurs as (
    select * from {{ ref('financeur') }}
),

reference as (
    select max(date_inscription)::date as dernier_jour
    from {{ ref('engagements_enfants_par_financeur') }}
    where compte_comme_inscrit
),

comptes as (
    select
        e.financeur_code,
        e.annee,
        count_if(e.compte_comme_inscrit)                as enfants_inscrits,
        -- n'a de sens que sur l'annee en cours : c'est le rythme de la projection
        count_if(e.compte_comme_inscrit
                 and e.date_inscription::date > r.dernier_jour - 56)
                                                        as enfants_inscrits_56_jours,
        count_if(e.est_entre_en_cohorte)                as enfants_entres_en_cohorte,
        count_if(e.est_reporte)                         as enfants_reportes
    from {{ ref('engagements_enfants_par_financeur') }} as e
    cross join reference as r
    group by e.financeur_code, e.annee
),

lignes_feuille as (
    select
        2026                        as annee,
        trim(par_dpartements)       as project_label,
        object_construct(*)         as ligne
    from {{ source('projects', 'export_data_2026') }}
    where nullif(trim(par_dpartements), '') is not null
),

statuts as (
    select
        annee,
        project_label,
        max(nullif(trim(get_ignore_case(ligne, 'statut')::varchar), '')) as statut
    from lignes_feuille
    group by annee, project_label
),

feuille as (
    select
        f.financeur_code,
        e.annee,
        sum(e.nb_enfants_engages)                       as engagement_partenaire,
        sum(e.nb_enfants_projetes)                      as projection_feuille,
        nullif(listagg(distinct s.statut, ' / ')
                   within group (order by s.statut), '') as statut,
        listagg(e.project_label, ' + ')
            within group (order by e.project_label)     as lignes_feuille
    from {{ ref('stg_projects__engagements') }} as e
    inner join financeurs as f
        on array_contains(e.project_code::variant, f.codes_feuille)
    left join statuts as s
        on s.annee = e.annee
       and s.project_label = e.project_label
    group by f.financeur_code, e.annee
),

annees as (
    select annee from comptes
    union
    select annee from feuille
)

select
    a.annee,
    f.financeur,

    -- ------------------------------------------------------ saisi dans la feuille
    fe.statut,
    fe.engagement_partenaire,

    -- ------------------------------------------------------------------ calcule
    case
        when a.annee < year(r.dernier_jour)
            then coalesce(c.enfants_inscrits, 0)
        when a.annee = year(r.dernier_jour) and dayofyear(r.dernier_jour) >= 28
            then coalesce(c.enfants_inscrits, 0)
                 + round(coalesce(c.enfants_inscrits_56_jours, 0)
                         / least(56, dayofyear(r.dernier_jour))
                         * datediff('day', r.dernier_jour,
                                    date_from_parts(a.annee, 12, 31)))
    end                                                 as projection,
    fe.projection_feuille,

    -- ------------------------------------------------------------ face a l'engagement
    coalesce(c.enfants_inscrits, 0)                     as enfants_inscrits,
    greatest(0, fe.engagement_partenaire - coalesce(c.enfants_inscrits, 0))
                                                        as places_restantes,
    round(100 * coalesce(c.enfants_inscrits, 0)
          / nullif(fe.engagement_partenaire, 0))        as pct_atteinte,

    -- ------------------------------------------------------- les enfants en cohorte
    coalesce(c.enfants_entres_en_cohorte, 0)            as enfants_entres_en_cohorte,
    coalesce(c.enfants_reportes, 0)                     as enfants_reportes,
    coalesce(c.enfants_entres_en_cohorte, 0)
        + coalesce(c.enfants_reportes, 0)               as enfants_inscrits_ou_accompagnes,

    -- ---------------------------------------------------------------- referentiel
    f.ordre,
    f.financeur_court,
    f.departement,
    f.regle_departement,
    f.regle_canal,
    f.regle_source,
    f.methode_de_calcul,
    fe.lignes_feuille,
    f.financeur_code
from financeurs as f
cross join annees as a
cross join reference as r
left join comptes as c
    on c.financeur_code = f.financeur_code
   and c.annee = a.annee
left join feuille as fe
    on fe.financeur_code = f.financeur_code
   and fe.annee = a.annee
