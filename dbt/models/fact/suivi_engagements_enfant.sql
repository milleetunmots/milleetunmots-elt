-- Detail du suivi des engagements : une ligne par enfant ET PAR ANNEE DE SUIVI.
--
-- Un engagement de 2026 ne se compte pas en inscriptions de 2026. L'annee de
-- suivi melange trois millesimes :
--   - des enfants dont l'accompagnement avait commence avant le 1er janvier et
--     se poursuit (ils viennent de l'annee precedente) ;
--   - des enfants dont la cohorte demarre dans l'annee ;
--   - des enfants inscrits dans l'annee dont l'accompagnement ne commencera que
--     l'annee suivante, cohorte deja planifiee ou pas encore.
-- Une cinquieme categorie, « E. inscrit, aucun accompagnement dans l'annee »,
-- recueille les enfants inscrits dans l'annee qui n'ont aucun jour de cycle
-- dedans : 4 700 enfants depuis 2020, dont 97 inscrits en 2026. Ils ne comptent
-- pour aucun engagement, mais ils restent dans la table pour que la perte soit
-- chiffrable.
--
-- La colonne categorie_suivi rend cette ventilation explicite, et
-- rattachement_temporel la resume en trois valeurs. quota_annee dit, lui, a
-- quel point l'enfant a compte DANS l'annee : 1 s'il a consomme une place
-- entiere, 0,25 s'il n'a demarre qu'en octobre. C'est la repartition du quota
-- entre les annees, en jours reels plutot qu'en paliers 1 / 0,8 / 0,6 comme le
-- fait accompagnement_annee_n_1_ajuste pour la projection N+1 de mecene : meme
-- intention, mais sans effet de seuil entre deux cohortes voisines. C'est la meme decoupe que
-- les macros accompagnement_annee_n_* lues par mecene, mais bornee par une
-- annee passee en clair plutot que par current_date : la ligne 2026 ne bougera
-- donc plus au 1er janvier 2027.
--
-- ATTENTION : un meme enfant apparait sur plusieurs annees de suivi, c'est le
-- principe. Ne jamais sommer les annees ; toujours filtrer sur une annee_suivi.
--
-- La maille est l'ENFANT, unite des engagements pris avec les financeurs
-- (« 30 places », « 20 inscriptions »), et chaque enfant est rattache a SA
-- source : une fratrie peut venir de deux sources differentes. Le nombre de
-- familles reste calculable en count(distinct family_id), jamais en count(*).
--
-- Les indicateurs d'appel, eux, sont de maille FAMILLE (un accompagnement par
-- famille) et repris telles quelles de ref('bureau') -- deux appels OK parmi
-- call0..call3, petite mission essayee ou reussie -- pour qu'aucun chiffre ne
-- puisse diverger avec le dashboard bureau. Chaque enfant d'une fratrie porte
-- donc le meme deux_appels_ok : c'est voulu.
--
-- Attention a la maille de bureau : 36 668 lignes pour 34 520 familles. Une
-- famille dont les enfants sont partis dans deux cohortes, ou dont un enfant
-- s'est desengage et l'autre pas, y apparait plusieurs fois. On ne reprend donc
-- de bureau que ses colonnes de maille famille (celles qui viennent de
-- child_supports), agregees par family_id ; cohorte, departement et date de fin
-- d'accompagnement sont relus dans les dimensions.
--
-- Trois codes de projet ne designent pas un financeur mais une sortie de
-- perimetre, et sont la pour que rien ne disparaisse en silence : 'TEST' (source
-- de test), 'BAO' (bouche-a-oreille, que personne ne finance) et 'SANS_SOURCE'
-- (source non renseignee, 39 enfants de 2023 a 2025).
--
-- Les colonnes is_* ne filtrent rien ici : l'agregat suivi_engagements decide
-- ce qu'il exclut, le detail reste auditable.

with bureau as (
    select * from {{ ref('bureau') }}
),

child as (
    select * from {{ ref('child') }}
),

parent as (
    select * from {{ ref('parent') }}
),

children_sources as (
    select * from {{ ref('stg_1001mots_app__children_sources') }}
),

source_project as (
    select * from {{ ref('source_project') }}
),

groups as (
    select * from {{ ref('groups') }}
),

versions as (
    select * from {{ ref('stg_1001mots_app__versions') }}
),

-- Indicateurs bureau ramenes a la maille famille.
indicateurs_bureau as (
    select
        family_id,
        max(deux_appels_ok)                       as deux_appels_ok,
        max(deux_appels_ok_plus_une_pm)           as deux_appels_ok_plus_une_pm,
        max(nb_pm_tried_or_succeed)               as nb_pm_tried_or_succeed,
        max(is_call0_ok)                          as is_call0_ok,
        max(is_call1_ok)                          as is_call1_ok,
        max(is_call2_ok)                          as is_call2_ok,
        max(is_call3_ok)                          as is_call3_ok,
        max(retention)                            as retention
    from bureau
    group by family_id
),

-- Sortie de cohorte : meme lecture que mecene, la date du passage de
-- group_status a 'stopped' ou 'disengaged' dans l'historique PaperTrail.
desengagement as (
    select
        item_id as child_id,
        max(to_date(nullif(parse_json(object_changes):"updated_at"[0]::string, ''))) as date_desengagement
    from versions
    where item_type = 'Child'
      and parse_json(object_changes):"group_status"[1]::string in ('stopped', 'disengaged')
    group by item_id
),

enfant as (
    select
        c.child_id,
        c.family_id,
        coalesce(sp.project_code, 'SANS_SOURCE') as project_code,
        coalesce(sp.project_label, 'Source non renseignee') as project_label,
        cs.source_id,
        sp.source_name,
        sp.channel                      as source_channel,
        sp.departement                  as source_departement,
        coalesce(sp.is_hors_feuille, false) as is_hors_feuille,
        p.departement                   as famille_departement,
        c.date_created                  as date_inscription,
        year(c.date_created)            as annee_inscription,
        cs.is_reenrollment,
        c.child_age_in_month,
        c.group_status,
        c.group_id,
        g.group_name                    as cohorte,
        g.date_started                  as date_debut_accompagnement,
        -- fenetre administrative de la cohorte : 343 jours, soit le double du
        -- cycle reel. Conservee pour audit, elle ne sert pas au quota.
        g.date_ended_clean              as date_fin_cohorte,
        -- fin du cycle des quatre appels : 174 jours apres le debut, stable sur
        -- toutes les cohortes depuis 2019 et toujours renseignee. C'est la duree
        -- d'une place, donc le denominateur du quota.
        g.date_call3_end                as date_fin_cycle,
        -- fin reelle : la fin de cohorte, ou le desengagement s'il est arrive
        -- avant. Meme calcul que ended_at_perso dans mecene.
        least(
            g.date_ended_clean,
            coalesce(d.date_desengagement, g.date_ended_clean)
        )                               as date_fin_accompagnement,
        -- fin retenue pour le quota : la fin reelle, plafonnee a la fin du cycle.
        -- Le statut 'stopped' est pose en moyenne 232 jours apres le debut, soit
        -- deux mois apres le dernier appel : sans ce plafond, un enfant alle au
        -- bout de son cycle vaudrait 1,33 place.
        least(
            g.date_call3_end,
            coalesce(d.date_desengagement, g.date_call3_end)
        )                               as date_fin_quota,
        g.date_call1_end,
        coalesce(g.is_excluded_from_analytics, false) as is_cohorte_exclue
    from child as c
    join children_sources as cs
        on cs.child_id = c.child_id
    -- jointure externe, et non interne : 39 enfants accompagnes entre 2023 et
    -- 2025 portent une ligne children_sources dont le source_id est nul. Aucun
    -- financeur n'est determinable pour eux, mais une jointure interne les
    -- faisait disparaitre du suivi sans laisser de trace. Ils sortent donc du
    -- perimetre par un code explicite, comme les sources de test.
    left join source_project as sp
        on sp.source_id = cs.source_id
    left join parent as p
        on p.parent_id = c.parent1_id
    left join groups as g
        on g.group_id = c.group_id
    left join desengagement as d
        on d.child_id = c.child_id
    -- les enfants supprimes de l'application ne sont comptes pour personne
    where c.date_discarded is null
),

-- Bornes de la plage d'annees, lues dans les donnees plutot que codees en dur :
-- l'invariant de couverture (tout enfant apparait au moins une fois) en depend,
-- et une constante le casserait silencieusement si des annees anterieures
-- etaient reprises un jour.
bornes as (
    select least(
               min(year(date_inscription)),
               coalesce(min(year(date_debut_accompagnement)), 9999)
           ) as premiere_annee
    from enfant
),

annees as (
    select annee_suivi
    from (
        select b.premiere_annee + seq4() as annee_suivi
        from table(generator(rowcount => 40))
        cross join bornes as b
    )
    where annee_suivi <= year(current_date) + 1
),

ventile as (
    select
        e.*,
        a.annee_suivi,
        date_from_parts(a.annee_suivi, 1, 1)   as debut_annee,
        date_from_parts(a.annee_suivi, 12, 31) as fin_annee,
        case
            -- accompagnement encore ouvert au 1er janvier : vient de l'annee
            -- precedente. La borne est date_fin_accompagnement, c'est-a-dire la
            -- fin de cohorte ou la cloture du statut si elle est arrivee avant --
            -- la meme que ended_at_perso dans mecene, pour que le bureau lise ici
            -- le chiffre qu'il lit deja la : 3 954 enfants reportes de 2025 sur
            -- 2026, et non 2 163.
            --
            -- Consequence a connaitre : le quota, lui, se borne sur le cycle des
            -- quatre appels (date_fin_quota). Un enfant dont le cycle s'est
            -- acheve l'annee precedente mais dont le statut est reste ouvert est
            -- donc un report qui ne consomme aucune place -- 1 791 des 3 954 en
            -- 2026. Ce n'est pas une incoherence : la portee compte une presence
            -- dans le programme, le quota des jours d'appel. La colonne
            -- is_report_sans_place les rend visibles.
            when e.date_debut_accompagnement < date_from_parts(a.annee_suivi, 1, 1)
             and e.date_fin_accompagnement   >= date_from_parts(a.annee_suivi, 1, 1)
                then 'A. accompagnement en cours au 1er janvier'
            -- cohorte demarree dans l'annee
            when year(e.date_debut_accompagnement) = a.annee_suivi
                then 'B. accompagnement commence dans l''annee'
            -- inscrit dans l'annee, cohorte planifiee sur l'annee suivante
            when year(e.date_inscription) = a.annee_suivi
             and year(e.date_debut_accompagnement) = a.annee_suivi + 1
                then 'C. inscrit, accompagnement l''annee suivante'
            -- inscrit dans l'annee, aucune cohorte planifiee
            when year(e.date_inscription) = a.annee_suivi
             and e.group_status = 'waiting'
                then 'D. inscrit, cohorte non planifiee'
            -- inscrit dans l'annee, mais aucun jour de cycle dans l'annee :
            -- statut not_supported, vieille cohorte sans date de demarrage, ou
            -- -- 4 cas sur 97 en 2026 -- cycle deja acheve l'annee d'avant alors
            -- que la fiche enfant a ete creee cette annee. Ces enfants ne
            -- comptent pour aucun engagement, mais ils ne doivent pas disparaitre
            -- sans laisser de trace : ils sortent du total par la categorie, pas
            -- par un filtre invisible.
            when year(e.date_inscription) = a.annee_suivi
                then 'E. inscrit, aucun accompagnement dans l''annee'
        end as categorie_suivi,
        -- Part de l'accompagnement de cet enfant qui tombe dans l'annee : c'est
        -- le « en partie » rendu mesurable, et la repartition du quota entre les
        -- annees. Un enfant dont la cohorte demarre en novembre ne pese pas, sur
        -- l'annee, autant qu'un enfant accompagne de janvier a decembre. 0 pour
        -- les categories C, D et E, dont l'accompagnement n'a pas commence.
        --
        -- Numerateur : les jours reellement accompagnes dans l'annee, bornes par
        -- le desengagement s'il y en a eu.
        greatest(
            0,
            datediff(
                'day',
                greatest(e.date_debut_accompagnement, date_from_parts(a.annee_suivi, 1, 1)),
                least(e.date_fin_quota, date_from_parts(a.annee_suivi, 12, 31))
            ) + 1
        ) as nb_jours_suivi_dans_annee,
        -- Meme chose arretee a aujourd'hui : c'est le quota consomme a date, le
        -- seul comparable au temps ecoule. Pour une annee passee, les deux se
        -- confondent.
        greatest(
            0,
            datediff(
                'day',
                greatest(e.date_debut_accompagnement, date_from_parts(a.annee_suivi, 1, 1)),
                least(e.date_fin_quota, date_from_parts(a.annee_suivi, 12, 31), current_date)
            ) + 1
        ) as nb_jours_suivi_a_date,
        -- Denominateur : le cycle des quatre appels de la cohorte, 174 jours.
        -- Un enfant alle au bout de son cycle dans une seule annee vaut donc une
        -- place pleine ; un enfant desengage a 86 jours en vaut la moitie ; un
        -- cycle a cheval sur deux annees se partage entre elles.
        --
        -- Ni la fenetre de cohorte (343 jours, administrative : elle raboterait
        -- le quota de moitie), ni la duree reelle de chaque enfant (un enfant
        -- qui decroche au bout de trois semaines vaudrait alors une place
        -- pleine, puisqu'il serait son propre denominateur).
        greatest(
            1,
            datediff('day', e.date_debut_accompagnement, e.date_fin_cycle) + 1
        ) as nb_jours_cycle
    from enfant as e
    cross join annees as a
)

select
    -- cle de la table : un enfant compte pour plusieurs annees de suivi
    concat(v.child_id, '_', v.annee_suivi)      as suivi_enfant_id,
    v.child_id,
    v.family_id,
    v.annee_suivi,
    v.categorie_suivi,
    -- resume de la ventilation en trois temps, tel qu'il se lit en seance
    case
        when v.categorie_suivi like 'A.%' then 'annee_precedente'
        when v.categorie_suivi like 'B.%' then 'annee_courante'
        when v.categorie_suivi like 'C.%' then 'annee_suivante'
        when v.categorie_suivi like 'D.%' then 'non_planifie'
        when v.categorie_suivi like 'E.%' then 'non_accompagne'
    end as rattachement_temporel,
    v.project_code,
    v.project_label,
    v.source_id,
    v.source_name,
    v.source_channel,
    v.source_departement,
    v.famille_departement,
    v.date_inscription,
    v.annee_inscription,
    -- l'enfant a-t-il ete inscrit dans l'annee de suivi, ou avant ?
    v.annee_inscription = v.annee_suivi             as is_inscrit_dans_annee,
    -- l'enfant est-il ENTRE dans le programme cette annee-la, c'est-a-dire sa
    -- cohorte a-t-elle demarre dans l'annee ? C'est la definition de la mesure
    -- « entrees » : une entree vaut une unite entiere, qu'elle soit allee au
    -- bout de son cycle ou non. Chaque enfant n'est donc entre qu'une seule
    -- fois, sur une seule annee : contrairement a la portee, qui le compte sur
    -- deux annees consecutives, et au quota, qui le partage entre elles, les
    -- entrees se somment entre annees sans double comptage.
    v.categorie_suivi like 'B.%'                    as is_entree_dans_annee,
    -- l'enfant a-t-il effectivement integre une cohorte, et celle-ci tourne-t-elle
    -- dans l'annee ? Vrai pour les reports de l'annee precedente (categorie A) et
    -- pour les entrees de l'annee (categorie B), faux pour les inscrits dont la
    -- cohorte est planifiee plus tard, pas planifiee, ou jamais venue. C'est le
    -- perimetre de la mesure « enfants en cohorte », ou chacun compte pour un.
    v.categorie_suivi like 'A.%'
      or v.categorie_suivi like 'B.%'               as is_en_cohorte_dans_annee,
    -- report dont le cycle d'appels s'etait deja acheve au 1er janvier : il
    -- compte dans la portee, il ne consomme aucune place. C'est l'ecart entre la
    -- convention de comptage (statut) et celle du quota (cycle).
    v.categorie_suivi like 'A.%'
      and v.nb_jours_suivi_dans_annee = 0           as is_report_sans_place,
    v.is_reenrollment,
    v.child_age_in_month,
    v.cohorte,
    v.date_debut_accompagnement,
    v.date_fin_accompagnement,
    v.group_status,
    v.date_fin_cohorte,
    v.date_fin_cycle,
    v.date_fin_quota,
    v.nb_jours_suivi_dans_annee,
    v.nb_jours_suivi_a_date,
    v.nb_jours_cycle,
    -- Quota : 1 = une place pleine consommee dans l'annee, 0,25 = un quart de
    -- place. 0, et non null, quand aucun accompagnement n'est date : ces enfants
    -- pesent zero dans l'annee, ce n'est pas une inconnue.
    coalesce(
        round(div0(v.nb_jours_suivi_dans_annee, v.nb_jours_cycle), 4),
        0
    ) as quota_annee,
    -- Le meme, arrete a aujourd'hui.
    coalesce(
        round(div0(v.nb_jours_suivi_a_date, v.nb_jours_cycle), 4),
        0
    ) as quota_annee_a_date,

    -- regles de comptabilisation du dashboard bureau (maille famille)
    ib.deux_appels_ok,
    ib.deux_appels_ok_plus_une_pm,
    ib.nb_pm_tried_or_succeed,
    ib.is_call0_ok,
    ib.is_call1_ok,
    ib.is_call2_ok,
    ib.is_call3_ok,
    ib.retention,

    -- La fenetre d'appel 1 de la cohorte est fermee : le deux_appels_ok de la
    -- famille est mesurable. Sans ce flag, toute cohorte recente fait chuter le
    -- taux. Les categories C et D ne sont jamais mesurables, par construction.
    coalesce(v.date_call1_end < current_date, false) as is_mesurable,
    v.date_call1_end,

    -- Perimetre
    v.project_code = 'TEST'                       as is_source_test,
    v.project_code = 'BAO'                        as is_sans_financeur,
    v.is_hors_feuille,
    v.is_cohorte_exclue,
    v.group_id is null                            as is_sans_cohorte

from ventile as v
left join indicateurs_bureau as ib
    on ib.family_id = v.family_id
-- une ligne par enfant et par annee ou il compte pour un financeur
where v.categorie_suivi is not null
