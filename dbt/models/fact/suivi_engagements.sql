-- Suivi des engagements du bureau : une ligne par projet (= financeur) et par
-- annee de suivi. L'engagement et la projection de la feuille du bureau, face a
-- quatre mesures de ce qu'on a reellement fait, qui ne disent pas la meme chose :
--
--   nb_enfants_suivis    le nombre d'enfants accompagnes au moins un jour dans
--                        l'annee. Mesure de portee : c'est le nombre d'enfants
--                        a qui 1001 mots a parle.
--   quota_consomme       la somme des parts d'accompagnement tombant dans
--                        l'annee (colonne quota_annee du detail). Mesure
--                        comptable : une place pleine vaut 1, un enfant demarre
--                        en novembre vaut 0,15, un enfant desengage au tiers de
--                        sa cohorte vaut 0,33. C'est elle qui se compare au
--                        quota vendu.
--   nb_enfants_en_cohorte  les enfants effectivement en cohorte dans l'annee,
--                        chacun pour une unite : les reports de l'annee
--                        precedente plus les entrees de l'annee. C'est la portee
--                        restreinte a ceux qui ont vraiment integre une cohorte.
--
-- Deux conventions cohabitent, et il faut le savoir : le COMPTAGE borne
-- l'accompagnement sur le statut de l'application, comme mecene, pour que le
-- bureau retrouve ses chiffres ; le QUOTA le borne sur le cycle des quatre
-- appels. D'ou nb_reports_sans_place : 1 791 enfants reportes en 2026 dont le
-- cycle etait clos, qui comptent dans la portee sans consommer de place.
--   nb_enfants_annee_courante  les entrees de l'annee, comptees une unite
--                        chacune : une famille qui entre dans le programme est
--                        une famille accompagnee, qu'elle soit allee au bout de
--                        son cycle ou non. nb_familles_entrees porte le meme
--                        compte en familles.
--
-- Les quatre se lisent ensemble : 10 078 enfants touches en 2026, dont 3 954
-- reportes de 2025, 6 020 entres dans l'annee, et 3 779 places consommees. Ne jamais presenter l'une pour l'autre, et ne jamais les
-- additionner.
--
-- Ce qui les distingue est le sort d'un enfant a cheval sur deux annees : la
-- portee le compte dans les deux, le quota le partage entre elles, les entrees
-- le comptent une fois, l'annee de son entree. Les entrees sont donc la seule
-- des trois qui se somme entre annees sans double comptage.
--
-- La repartition du quota entre les annees se fait au prorata des jours reels
-- (voir suivi_engagements_enfant), et non par les paliers 1 / 0,8 / 0,6 de
-- accompagnement_annee_n_1_ajuste : meme intention, sans effet de seuil entre
-- deux cohortes voisines.
--
-- Les taux sont ecrits en division protegee par nullif, et non avec div0null :
-- div0null renvoie 0 quand le denominateur est nul, ce qui ferait afficher
-- « 0 % atteint » a un projet qui n'a aucun engagement -- un retard la ou il n'y
-- a pas d'objectif.
--
-- Trois rythmes se comparent :
--   taux_atteinte_quota      quota consomme a date / quota vendu
--   taux_avancement_temps    part de l'annee ecoulee
--   ecart_rythme             la difference, en points. Negatif = en retard sur
--                            le calendrier.
--
-- La projection de fin d'annee est en deux morceaux, pour qu'on sache ce qui
-- est acquis et ce qui est parie :
--   quota_acquis_fin_annee   les enfants DEJA inscrits en cohorte, comptes
--                            jusqu'au 31 decembre. Deterministe.
--   quota_projete_fin_annee  l'acquis plus les entrees encore attendues d'ici
--                            la fin de l'annee. La methode est dans
--                            methode_projection.
--
-- Un meme enfant compte sur deux annees consecutives (une fois en « annee
-- courante », une fois en « annee precedente ») : ne jamais sommer les annees.
--
-- Le grain couvre toutes les annees presentes dans le detail, pas seulement
-- celles que la feuille du bureau renseigne : un financeur a une ligne par annee
-- ou des enfants lui sont rattaches, avec un quota_vendu nul tant que la feuille
-- ne porte pas cette annee-la. C'est ce qui permet de comparer 2024, 2025 et
-- 2026 a perimetre constant, et de poser un engagement par annee le jour ou la
-- feuille en portera.
--
-- Sont exclues du comptage les sources de test et les cohortes exclues de
-- l'analytics. Le bouche-a-oreille et les reinscriptions directes (code 'BAO')
-- restent visibles sur leur propre ligne, sans engagement.

with engagements as (
    select * from {{ ref('stg_projects__engagements') }}
),

-- Le libelle, le type de financeur et le departement viennent de la feuille du
-- bureau, qui ne porte qu'une annee. On les rattache au code de projet plutot
-- qu'a la ligne (projet, annee), sinon toutes les annees que la feuille ne
-- couvre pas afficheraient un libelle vide. Le libelle le plus recent gagne.
identite_projet as (
    select
        project_code,
        max_by(project_label, annee)   as project_label,
        max_by(financeur_type, annee)  as financeur_type,
        max_by(departement, annee)     as departement
    from engagements
    group by project_code
),

enfants as (
    select
        *,
        rattachement_temporel != 'non_accompagne' as is_suivi
    from {{ ref('suivi_engagements_enfant') }}
    where not is_source_test
      and not is_cohorte_exclue
),

comptes as (
    select
        e.project_code,
        e.annee_suivi                                           as annee,
        count_if(e.is_suivi)                                    as nb_enfants_suivis,
        count(distinct case when e.is_suivi then e.family_id end) as nb_familles_suivies,

        -- ventilation des trois millesimes : ces quatre colonnes somment a
        -- nb_enfants_suivis
        count_if(e.rattachement_temporel = 'annee_precedente')  as nb_enfants_annee_precedente,
        -- dont ceux dont le cycle d'appels etait deja clos : ils comptent dans
        -- la portee sans consommer de place
        count_if(e.is_report_sans_place)                        as nb_reports_sans_place,
        count_if(e.rattachement_temporel = 'annee_courante')    as nb_enfants_annee_courante,
        count_if(e.rattachement_temporel = 'annee_suivante')    as nb_enfants_annee_suivante,
        count_if(e.rattachement_temporel = 'non_planifie')      as nb_enfants_non_planifie,
        -- hors total : inscrits dans l'annee, jamais pris en accompagnement
        count_if(e.rattachement_temporel = 'non_accompagne')    as nb_enfants_non_accompagnes,

        count_if(e.is_inscrit_dans_annee and e.is_suivi)        as nb_enfants_inscrits_dans_annee,
        count_if(e.is_suivi and e.rattachement_temporel != 'annee_precedente')
                                                                as nb_enfants_nouveaux,

        -- enfants effectivement en cohorte dans l'annee, chacun pour une unite :
        -- les reports de l'annee precedente plus les entrees de l'annee. Exclut
        -- les inscrits dont la cohorte est planifiee plus tard, pas planifiee ou
        -- jamais venue -- soit 104 enfants sur 8 287 en 2026.
        count_if(e.is_en_cohorte_dans_annee)                    as nb_enfants_en_cohorte,
        count(distinct case when e.is_en_cohorte_dans_annee then e.family_id end)
                                                                as nb_familles_en_cohorte,

        -- entrees : une cohorte demarree dans l'annee vaut une unite entiere,
        -- sans prorata ni decote d'attrition. nb_enfants_annee_courante porte le
        -- compte en enfants, celle-ci le meme en familles.
        count(distinct case when e.is_entree_dans_annee then e.family_id end)
                                                                as nb_familles_entrees,

        -- les deux faces du quota
        round(sum(e.quota_annee_a_date), 1)                     as quota_consomme,
        round(sum(e.quota_annee), 1)                            as quota_acquis_fin_annee,
        round(sum(case when e.rattachement_temporel = 'annee_precedente'
                       then e.quota_annee else 0 end), 1)       as quota_annee_precedente,
        round(sum(case when e.rattachement_temporel = 'annee_courante'
                       then e.quota_annee else 0 end), 1)       as quota_annee_courante,

        -- indicateurs d'accompagnement, regles du dashboard bureau, sur les
        -- seuls enfants effectivement suivis dans l'annee
        sum(case when e.is_suivi then e.is_call0_ok else 0 end) as nb_enfants_call0_ok,
        sum(case when e.is_suivi then e.deux_appels_ok else 0 end)
                                                                as nb_enfants_deux_appels_ok,
        sum(case when e.is_suivi then e.deux_appels_ok_plus_une_pm else 0 end)
                                                                as nb_enfants_deux_appels_ok_une_pm,
        count_if(e.is_suivi and e.is_mesurable)                 as nb_enfants_mesurables,
        sum(case when e.is_suivi and e.is_mesurable then e.deux_appels_ok else 0 end)
                                                                as nb_enfants_deux_appels_ok_mesurables,
        sum(case when e.is_suivi and e.is_mesurable then e.deux_appels_ok_plus_une_pm else 0 end)
                                                                as nb_enfants_deux_appels_ok_une_pm_mesurables
    from enfants as e
    group by e.project_code, e.annee_suivi
),

-- Ou tombe le cycle des enfants comptes sur l'annee. Un enfant entre en
-- novembre est bien une entree de l'annee -- il compte dans sa portee -- mais
-- l'essentiel de son cycle tombera l'annee suivante. Ces deux colonnes disent
-- combien d'enfants sont dans ce cas et combien de places ils reportent.
cycle_par_enfant as (
    select
        compte.project_code,
        compte.annee,
        compte.child_id,
        sum(case when tous.annee_suivi = compte.annee - 1 then tous.quota_annee else 0 end) as quota_n_moins_1,
        sum(case when tous.annee_suivi = compte.annee     then tous.quota_annee else 0 end) as quota_n,
        sum(case when tous.annee_suivi = compte.annee + 1 then tous.quota_annee else 0 end) as quota_n_plus_1
    from (
        select project_code, annee_suivi as annee, child_id
        from enfants
        where is_suivi
    ) as compte
    join enfants as tous
        on tous.child_id = compte.child_id
    group by compte.project_code, compte.annee, compte.child_id
),

report_annee_suivante as (
    select
        project_code,
        annee,
        count_if(quota_n_plus_1 > greatest(quota_n_moins_1, quota_n))
                                                        as nb_enfants_cycle_surtout_annee_suivante,
        round(sum(quota_n_plus_1), 1)                   as quota_reporte_annee_suivante,
        round(sum(quota_n_moins_1), 1)                  as quota_deja_delivre_annee_precedente
    from cycle_par_enfant
    group by project_code, annee
),

-- Entrees en cohorte, coupees a la date du jour reportee sur chaque annee :
-- de quoi comparer une annee en cours a la meme periode des annees passees.
entrees as (
    select
        project_code,
        annee_suivi                                             as annee,
        count_if(date_debut_accompagnement
                 <= date_from_parts(annee_suivi, month(current_date), day(current_date)))
                                                                as nb_entrees_a_date,
        count_if(date_debut_accompagnement
                 > date_from_parts(annee_suivi, month(current_date), day(current_date)))
                                                                as nb_entrees_fin_annee,
        sum(case when date_debut_accompagnement
                      > date_from_parts(annee_suivi, month(current_date), day(current_date))
                 then quota_annee else 0 end)                   as quota_entrees_fin_annee
    from enfants
    where is_suivi
      and rattachement_temporel = 'annee_courante'
    group by project_code, annee_suivi
),

-- Saisonnalite de la fin d'annee, mesuree sur l'annee precedente : combien
-- d'entrees en cohorte arrivent APRES la date du jour, rapportees a celles
-- deja enregistrees a cette date. 0,25 pour 2025, 0,17 pour 2024 : novembre et
-- decembre ne ressemblent pas a la rentree, un rythme hebdomadaire moyen les
-- surestimerait. Mesuree globalement, elle est stable ; projet par projet, elle
-- divise par de petits nombres et s'envole, d'ou le seuil de 50 entrees.
reference_saisonniere as (
    select
        div0null(sum(nb_entrees_fin_annee), sum(nb_entrees_a_date)) as part_tardive_globale,
        div0null(sum(quota_entrees_fin_annee), sum(nb_entrees_fin_annee)) as quota_moyen_tardif_global
    from entrees
    where annee = year(current_date) - 1
),

projection as (
    select
        e.project_code,
        e.annee,
        e.nb_entrees_a_date,
        e.nb_entrees_fin_annee                                  as nb_entrees_fin_annee_connues,
        prec.nb_entrees_a_date                                  as nb_entrees_a_date_an_precedent,
        -- part tardive du projet, seulement si son historique est assez epais
        case
            when prec.nb_entrees_a_date >= 50
                then div0null(prec.nb_entrees_fin_annee, prec.nb_entrees_a_date)
        end                                                     as part_tardive_projet,
        case
            when prec.nb_entrees_fin_annee >= 20
                then div0null(prec.quota_entrees_fin_annee, prec.nb_entrees_fin_annee)
        end                                                     as quota_moyen_tardif_projet
    from entrees as e
    left join entrees as prec
        on prec.project_code = e.project_code
       and prec.annee = e.annee - 1
),

base as (
    select
        coalesce(e.project_code, c.project_code)        as project_code,
        coalesce(e.annee, c.annee)                      as annee,
        coalesce(e.project_label, ip.project_label, c.project_code) as project_label,
        coalesce(e.financeur_type, ip.financeur_type)               as financeur_type,
        coalesce(e.departement, ip.departement)                     as departement,
        e.nb_enfants_engages,
        e.nb_enfants_projetes,

        coalesce(c.nb_enfants_suivis, 0)                as nb_enfants_suivis,
        coalesce(c.nb_familles_suivies, 0)              as nb_familles_suivies,
        coalesce(c.nb_enfants_annee_precedente, 0)      as nb_enfants_annee_precedente,
        coalesce(c.nb_reports_sans_place, 0)            as nb_reports_sans_place,
        coalesce(c.nb_enfants_annee_courante, 0)        as nb_enfants_annee_courante,
        coalesce(c.nb_enfants_annee_suivante, 0)        as nb_enfants_annee_suivante,
        coalesce(c.nb_enfants_non_planifie, 0)          as nb_enfants_non_planifie,
        coalesce(c.nb_enfants_non_accompagnes, 0)       as nb_enfants_non_accompagnes,
        coalesce(c.nb_enfants_inscrits_dans_annee, 0)   as nb_enfants_inscrits_dans_annee,
        coalesce(c.nb_enfants_nouveaux, 0)              as nb_enfants_nouveaux,
        coalesce(c.nb_familles_entrees, 0)              as nb_familles_entrees,
        coalesce(c.nb_enfants_en_cohorte, 0)            as nb_enfants_en_cohorte,
        coalesce(c.nb_familles_en_cohorte, 0)           as nb_familles_en_cohorte,

        coalesce(c.quota_consomme, 0)                   as quota_consomme,
        coalesce(c.quota_acquis_fin_annee, 0)           as quota_acquis_fin_annee,
        coalesce(c.quota_annee_precedente, 0)           as quota_annee_precedente,
        coalesce(c.quota_annee_courante, 0)             as quota_annee_courante,

        coalesce(c.nb_enfants_call0_ok, 0)              as nb_enfants_call0_ok,
        coalesce(c.nb_enfants_deux_appels_ok, 0)        as nb_enfants_deux_appels_ok,
        coalesce(c.nb_enfants_deux_appels_ok_une_pm, 0) as nb_enfants_deux_appels_ok_une_pm,
        coalesce(c.nb_enfants_mesurables, 0)            as nb_enfants_mesurables,
        coalesce(c.nb_enfants_deux_appels_ok_mesurables, 0)
                                                        as nb_enfants_deux_appels_ok_mesurables,
        coalesce(c.nb_enfants_deux_appels_ok_une_pm_mesurables, 0)
                                                        as nb_enfants_deux_appels_ok_une_pm_mesurables,

        ra.nb_enfants_cycle_surtout_annee_suivante,
        ra.quota_reporte_annee_suivante,
        ra.quota_deja_delivre_annee_precedente,
        p.nb_entrees_a_date,
        p.nb_entrees_fin_annee_connues,
        p.nb_entrees_a_date_an_precedent,
        coalesce(p.part_tardive_projet, r.part_tardive_globale) as part_tardive_reference,
        p.part_tardive_projet,
        coalesce(p.quota_moyen_tardif_projet, r.quota_moyen_tardif_global)
                                                        as quota_moyen_entree_tardive,

        e.project_code is null                          as is_hors_engagement,
        c.project_code is null                          as is_sans_suivi
    from engagements as e
    full outer join comptes as c
        on c.project_code = e.project_code
       and c.annee = e.annee
    left join projection as p
        on p.project_code = coalesce(e.project_code, c.project_code)
       and p.annee = coalesce(e.annee, c.annee)
    left join identite_projet as ip
        on ip.project_code = coalesce(e.project_code, c.project_code)
    left join report_annee_suivante as ra
        on ra.project_code = coalesce(e.project_code, c.project_code)
       and ra.annee = coalesce(e.annee, c.annee)
    cross join reference_saisonniere as r
),

-- Entrees encore attendues d'ici le 31 decembre, et quota qu'elles apporteront.
-- Priorite a la saisonnalite : les entrees de la meme fin d'annee l'an dernier,
-- corrigees de la croissance du projet. Novembre et decembre ne ressemblent pas
-- a la rentree, un rythme hebdomadaire moyen les surestimerait.
estimation as (
    select
        base.*,
        case
            when annee != year(current_date)      then 'annee_close'
            when coalesce(nb_entrees_a_date, 0) = 0 then 'aucune_entree'
            when part_tardive_projet is not null  then 'saisonnalite_projet'
            else 'saisonnalite_globale'
        end as methode_projection,
        -- entrees encore attendues : ce que la saisonnalite annonce pour la fin
        -- d'annee, moins les cohortes de fin d'annee deja planifiees, qui sont
        -- comptees dans l'acquis
        case
            when annee != year(current_date) then 0
            else greatest(
                0,
                round(coalesce(nb_entrees_a_date, 0) * coalesce(part_tardive_reference, 0))
                    - coalesce(nb_entrees_fin_annee_connues, 0)
            )
        end as nb_entrees_attendues_fin_annee
    from base
),

mesures as (
select
    project_code,
    annee,
    project_label,
    financeur_type,
    departement,

    -- ce qui a ete vendu
    nb_enfants_engages                              as quota_vendu,
    nb_enfants_projetes                             as quota_projete_bureau,

    -- portee : les enfants touches
    nb_enfants_suivis,
    nb_familles_suivies,
    nb_enfants_annee_precedente,
    nb_reports_sans_place,
    nb_enfants_annee_courante,
    nb_enfants_annee_suivante,
    nb_enfants_non_planifie,
    nb_enfants_non_accompagnes,
    nb_enfants_inscrits_dans_annee,
    nb_enfants_nouveaux,
    nb_familles_entrees,
    nb_enfants_en_cohorte,
    nb_familles_en_cohorte,

    -- comptabilite : les places consommees
    quota_consomme,
    quota_acquis_fin_annee,
    quota_annee_precedente,
    quota_annee_courante,

    -- ou tombe le cycle de ces enfants
    coalesce(nb_enfants_cycle_surtout_annee_suivante, 0) as nb_enfants_cycle_surtout_annee_suivante,
    coalesce(quota_reporte_annee_suivante, 0)       as quota_reporte_annee_suivante,
    coalesce(quota_deja_delivre_annee_precedente, 0) as quota_deja_delivre_annee_precedente,

    -- avancement
    quota_consomme / nullif(nb_enfants_engages, 0)  as taux_atteinte_quota,
    -- troisieme lecture : les entrees de l'annee, comptees une unite chacune.
    -- nb_enfants_annee_courante en est le numerateur.
    nb_enfants_annee_courante / nullif(nb_enfants_engages, 0) as taux_atteinte_entrees,
    -- quatrieme lecture : les enfants en cohorte dans l'annee, chacun pour un,
    -- reports de l'annee precedente compris.
    nb_enfants_en_cohorte / nullif(nb_enfants_engages, 0) as taux_atteinte_en_cohorte,
    nb_enfants_suivis / nullif(nb_enfants_engages, 0) as taux_atteinte_portee,
    case
        when annee < year(current_date) then 1
        when annee > year(current_date) then 0
        else round(
            (datediff('day', date_from_parts(annee, 1, 1), current_date) + 1)
            / (datediff('day', date_from_parts(annee, 1, 1), date_from_parts(annee, 12, 31)) + 1),
            4)
    end                                             as taux_avancement_temps,

    -- projection de fin d'annee
    methode_projection,
    nb_entrees_fin_annee_connues,
    nb_entrees_attendues_fin_annee,
    round(part_tardive_reference, 3)                as part_tardive_reference,
    round(quota_moyen_entree_tardive, 3)            as quota_moyen_entree_tardive,

    -- indicateurs d'accompagnement
    nb_enfants_call0_ok,
    nb_enfants_deux_appels_ok,
    nb_enfants_deux_appels_ok_une_pm,
    nb_enfants_mesurables,
    nb_enfants_deux_appels_ok_mesurables,
    nb_enfants_deux_appels_ok_une_pm_mesurables,
    nb_enfants_deux_appels_ok_mesurables / nullif(nb_enfants_mesurables, 0)
                                                    as taux_deux_appels_ok,
    nb_enfants_deux_appels_ok_une_pm_mesurables / nullif(nb_enfants_mesurables, 0)
                                                    as taux_deux_appels_ok_une_pm,

    quota_annee_precedente / nullif(quota_acquis_fin_annee, 0) as part_quota_annee_precedente,
    is_hors_engagement,
    is_sans_suivi
from estimation
)

select
    m.*,

    -- quota encore a venir : les entrees attendues, chacune ne delivrant que la
    -- fin de l'annee (0,1 place en moyenne pour une cohorte de novembre)
    round(m.nb_entrees_attendues_fin_annee * coalesce(m.quota_moyen_entree_tardive, 0), 1)
                                                    as quota_futur_estime,
    round(m.quota_acquis_fin_annee
        + m.nb_entrees_attendues_fin_annee * coalesce(m.quota_moyen_entree_tardive, 0), 1)
                                                    as quota_projete_fin_annee,
    m.nb_enfants_suivis + m.nb_entrees_attendues_fin_annee
                                                    as nb_enfants_suivis_projete,

    -- Projection des enfants en cohorte : les reports de l'annee precedente et
    -- les entrees deja faites sont acquis, seules les entrees encore attendues
    -- sont estimees. C'est la projection la plus solide des trois, parce que la
    -- part pariee y est la plus faible.
    m.nb_enfants_en_cohorte + m.nb_entrees_attendues_fin_annee
                                                    as nb_enfants_en_cohorte_projete,
    (m.nb_enfants_en_cohorte + m.nb_entrees_attendues_fin_annee)
        / nullif(m.quota_vendu, 0)                  as taux_atteinte_en_cohorte_projete,

    -- Avance ou retard sur le calendrier, en points de pourcentage : c'est la
    -- colonne a trier pour savoir qui relancer. Negatif = en retard.
    round(m.taux_atteinte_quota - m.taux_avancement_temps, 4)
                                                    as ecart_rythme,

    (m.quota_acquis_fin_annee
      + m.nb_entrees_attendues_fin_annee * coalesce(m.quota_moyen_entree_tardive, 0))
        / nullif(m.quota_vendu, 0)                  as taux_atteinte_quota_projete,
    (m.nb_enfants_suivis + m.nb_entrees_attendues_fin_annee) / nullif(m.quota_vendu, 0)
                                                    as taux_atteinte_portee_projete
from mesures as m
