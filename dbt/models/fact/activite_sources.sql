{{ config(materialized='table') }}

-- Activite des sources d'inscription : une ligne par source, avec un statut qui
-- dit s'il faut recontacter le partenaire. Sert l'alerte « sources a relancer ».
--
-- Le signal d'inactivite est RELATIF au rythme de la source, pas un nombre de
-- jours fixe. Une source est inactive quand son silence depasse 3 fois son
-- intervalle habituel entre deux jours d'inscription (365 / jours actifs sur
-- l'annee glissante), avec un plancher de 30 jours. Une PMI qui inscrit chaque
-- semaine est signalee apres un mois ; une qui inscrit tous les deux mois ne
-- l'est pas avant six.
--
-- Pourquoi pas un seuil absolu : l'activite est tres saisonniere (pics en
-- novembre, mars-mai, septembre-octobre ; creux l'ete), et 76 % des sources
-- silencieuses depuis 90 jours reviennent seules. Backtest du 2026-11-03, sur 15
-- photos mensuelles de mars 2025 a mai 2026, « vraie alerte » = aucune
-- inscription dans les 120 jours suivants :
--   absolu 90 jours        273 alertes, precision 0,63
--   relatif 3 x intervalle 221 alertes, precision 0,65, et ce sont les sources
--                          les plus volumineuses qui sont attrapees
-- Le signal relatif n'est calcule que pour les sources regulieres (au moins 4
-- jours d'inscription sur l'annee) : en deca, l'intervalle n'a pas de sens.
--
-- Statuts, dans l'ordre ou ils s'appliquent :
--   archivee         source archivee dans l'application, hors suivi
--   jamais_demarree  creee depuis 60 jours ou plus, aucune inscription
--   nouvelle         creee depuis moins de 60 jours, aucune inscription
--   dormante         rien depuis un an ou plus : relance deja trop tardive
--   inactive         silence >= seuil_silence_jours (signal backteste)
--   en_baisse        inscrit toujours, mais moins de 40 % de ses inscriptions
--                    des memes 180 jours l'an dernier, OU des memes 90 jours
--                    (au moins 10 alors, sur la fenetre en cause). Comparer a
--                    la meme periode neutralise la saisonnalite. Les 90 jours
--                    attrapent une chute recente que 180 jours diluent dans un
--                    printemps normal (PMI Villeneuve-la-Garenne, 2026-11-03 :
--                    0,51 sur 180 jours, 0,21 sur 90).
--                    Regle NON backtestee : a juger a l'usage.
--   occasionnelle    moins de 4 jours d'inscription sur l'annee
--   active           le reste
--
-- Contacts : les emails des professionnels qui ont inscrit des enfants via la
-- source, du plus recent au plus ancien. La saisie n'existe que depuis aout
-- 2026 et ne couvre qu'un tiers des inscriptions : une source silencieuse
-- depuis plus longtemps n'a souvent aucun email, il faut alors passer par le
-- referent du financeur.
--
-- Perimetre : celui de fact.inscriptions_sources -- hors reinscriptions, hors
-- enfants supprimes, hors sources de test et bouche-a-oreille.

with source_project as (
    select * from {{ ref('source_project') }}
),

sources as (
    select * from {{ ref('stg_1001mots_app__sources') }}
),

-- la definition d'une inscription vit dans fact.inscriptions_sources
inscriptions as (
    select * from {{ ref('inscriptions_sources') }}
),

activite as (
    select
        source_id,
        min(date_inscription)                                               as date_premiere_inscription,
        max(date_inscription)                                               as date_derniere_inscription,
        count(*)                                                            as nb_inscriptions_total,
        count(distinct iff(date_inscription > dateadd(day, -365, current_date),
                           date_inscription, null))                         as nb_jours_actifs_365j,
        count_if(date_inscription > dateadd(day, -365, current_date))       as nb_inscriptions_365j,
        count_if(date_inscription > dateadd(day, -90, current_date))        as nb_inscriptions_90j,
        count_if(date_inscription >  dateadd(day, -455, current_date)
             and date_inscription <= dateadd(day, -365, current_date))      as nb_inscriptions_90j_n1,
        count_if(date_inscription > dateadd(day, -180, current_date))       as nb_inscriptions_180j,
        count_if(date_inscription >  dateadd(day, -545, current_date)
             and date_inscription <= dateadd(day, -365, current_date))      as nb_inscriptions_180j_n1
    from inscriptions
    group by source_id
),

-- Les habitudes de la source, pour juger son silence actuel au regard de son
-- passe : un silence plus long que tout ce qu'elle a jamais fait, ou une
-- periode ou elle inscrivait l'an dernier, est plus parlant qu'un seuil.
jours_inscription as (
    select distinct source_id, date_inscription
    from inscriptions
),

ecarts as (
    select
        source_id,
        datediff(day, date_inscription,
                 lead(date_inscription) over (partition by source_id order by date_inscription)) as ecart_jours
    from jours_inscription
),

-- le silence en cours est exclu (lead null) : on le compare a ce plus long
-- silence passe, il ne doit pas en faire partie
plus_long_silence as (
    select source_id, max(ecart_jours) as plus_long_silence_passe_jours
    from ecarts
    group by source_id
),

-- les trois mois de l'annee ou la source inscrit le plus, tout historique
mois_forts as (
    select
        source_id,
        listagg(mois, ', ') within group (order by nb desc, num_mois) as mois_les_plus_actifs
    from (
        select
            source_id,
            month(date_inscription)                     as num_mois,
            decode(month(date_inscription), 1, 'janv', 2, 'fevr', 3, 'mars', 4, 'avr',
                   5, 'mai', 6, 'juin', 7, 'juil', 8, 'aout', 9, 'sept', 10, 'oct',
                   11, 'nov', 12, 'dec')                as mois,
            count(*)                                    as nb,
            row_number() over (partition by source_id order by count(*) desc, month(date_inscription)) as rang
        from inscriptions
        group by source_id, month(date_inscription), mois
    )
    where rang <= 3
    group by source_id
),

-- l'an dernier, sur la periode calendaire du silence en cours : si la source
-- inscrivait a cette epoque, le silence n'est pas saisonnier
meme_periode_n1 as (
    select
        a.source_id,
        count(i.child_id) as nb_inscriptions_meme_periode_n1
    from activite as a
    left join inscriptions as i
        on i.source_id = a.source_id
        and i.date_inscription >  dateadd(day, -365, a.date_derniere_inscription)
        and i.date_inscription <= dateadd(day, -365, current_date)
    group by a.source_id
),

-- un email par ligne, rang 1 = le pro qui a inscrit le plus recemment
emails as (
    select
        source_id,
        professional_email,
        max(date_inscription) as date_derniere_inscription_pro,
        count(*)              as nb_inscriptions_pro
    from inscriptions
    where professional_email is not null
    group by source_id, professional_email
),

contacts as (
    select
        source_id,
        count(*) as nb_contacts,
        listagg(professional_email, ', ')
            within group (order by date_derniere_inscription_pro desc) as emails_contacts,
        max_by(professional_email, date_derniere_inscription_pro)      as email_contact_principal
    from emails
    group by source_id
),

mesures as (
    select
        sp.source_id,
        sp.source_name,
        sp.channel,
        sp.departement,
        sp.project_code,
        sp.project_label,
        sp.is_archived,
        s.date_created as date_creation_source,
        a.date_premiere_inscription,
        a.date_derniere_inscription,
        coalesce(a.nb_inscriptions_total, 0)   as nb_inscriptions_total,
        coalesce(a.nb_jours_actifs_365j, 0)    as nb_jours_actifs_365j,
        coalesce(a.nb_inscriptions_365j, 0)    as nb_inscriptions_365j,
        coalesce(a.nb_inscriptions_90j, 0)     as nb_inscriptions_90j,
        coalesce(a.nb_inscriptions_90j_n1, 0)  as nb_inscriptions_90j_n1,
        coalesce(a.nb_inscriptions_180j, 0)    as nb_inscriptions_180j,
        coalesce(a.nb_inscriptions_180j_n1, 0) as nb_inscriptions_180j_n1,
        datediff(day, a.date_derniere_inscription, current_date) as jours_sans_inscription,
        a.nb_inscriptions_total / nullif(datediff(month, a.date_premiere_inscription, current_date) + 1, 0)
            as nb_inscriptions_par_mois_historique,
        pls.plus_long_silence_passe_jours,
        mf.mois_les_plus_actifs,
        mp.nb_inscriptions_meme_periode_n1,
        -- intervalle habituel entre deux jours d'inscription, null si trop peu
        -- de points pour en avoir un
        iff(a.nb_jours_actifs_365j >= 4, round(365 / a.nb_jours_actifs_365j), null)
            as intervalle_habituel_jours,
        iff(a.nb_jours_actifs_365j >= 4, greatest(30, ceil(3 * 365 / a.nb_jours_actifs_365j)), null)
            as seuil_silence_jours,
        datediff(day, s.date_created, current_date) as anciennete_source_jours,
        coalesce(c.nb_contacts, 0) as nb_contacts,
        c.email_contact_principal,
        c.emails_contacts
    from source_project as sp
    inner join sources as s
        on s.source_id = sp.source_id
    left join activite as a
        on a.source_id = sp.source_id
    left join contacts as c
        on c.source_id = sp.source_id
    left join plus_long_silence as pls
        on pls.source_id = sp.source_id
    left join mois_forts as mf
        on mf.source_id = sp.source_id
    left join meme_periode_n1 as mp
        on mp.source_id = sp.source_id
    where sp.project_code not in ('TEST', 'BAO')
)

select
    *,
    case
        when is_archived                                           then 'archivee'
        when date_derniere_inscription is null
         and anciennete_source_jours >= 60                         then 'jamais_demarree'
        when date_derniere_inscription is null                     then 'nouvelle'
        when jours_sans_inscription >= 365                         then 'dormante'
        when jours_sans_inscription >= seuil_silence_jours         then 'inactive'
        when nb_inscriptions_180j_n1 >= 10
         and nb_inscriptions_180j < 0.4 * nb_inscriptions_180j_n1  then 'en_baisse'
        when nb_inscriptions_90j_n1 >= 10
         and nb_inscriptions_90j < 0.4 * nb_inscriptions_90j_n1    then 'en_baisse'
        when nb_jours_actifs_365j < 4                              then 'occasionnelle'
        else 'active'
    end as statut,
    -- date a laquelle la source a franchi son seuil de silence : c'est elle qui
    -- permet a l'alerte de ne signaler chaque source qu'une fois. Approchee, car
    -- le seuil est recalcule chaque jour sur l'annee glissante.
    iff(jours_sans_inscription >= seuil_silence_jours and jours_sans_inscription < 365,
        dateadd(day, seuil_silence_jours, date_derniere_inscription), null)
        as date_bascule_inactive
from mesures
