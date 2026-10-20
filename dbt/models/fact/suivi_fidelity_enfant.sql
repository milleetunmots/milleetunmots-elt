-- Detail du suivi Fidelity : une ligne par enfant et par cohorte, portant les
-- flags de numerateur et de denominateur des trois indicateurs du contrat, plus
-- les deux etapes du dispositif de prise de RDV.
--
-- C'est la brique commune de suivi_fidelity (agregat annuel, la grille
-- contractuelle) et de kpi_fidelity_cohorte (la dynamique, cohorte par
-- cohorte) : les deux lisent les memes flags, de sorte qu'une definition ne
-- puisse pas deriver entre la grille et le graphique qu'on montre a cote.
--
-- MAILLE : l'enfant, comme suivi_engagements_enfant et comme tout le reporting
-- financeur de ce depot. Un engagement se contractualise en nombre d'enfants a
-- accompagner ; c'est donc en enfants que le financeur lit ce qu'il a finance,
-- et une fratrie de deux compte pour deux. super_table est deja exactement a
-- cette maille -- 28 288 lignes pour 28 288 enfants -- il n'y a donc aucune
-- agregation a faire ici, seulement des flags a poser.
--
-- Les flags vont par paire : appartenance au denominateur, appartenance au
-- numerateur, le numerateur toujours inclus dans son denominateur. Un taux se
-- calcule donc partout en sum(numerateur) / sum(denominateur) -- un ratio de
-- sommes, jamais une moyenne de taux : agreger des cohortes de 200 et de 550
-- enfants a poids egal fausserait l'annee.
--
-- Voir suivi_fidelity pour le choix des definitions et leur calage sur la
-- baseline 2024 de la grille.

with super_table as (
    select * from {{ ref('super_table') }}
),

groups as (
    select * from {{ ref('groups') }}
)

select
    st.child_id,
    st.family_id,
    st.cohort_name,
    year(g.date_call0_start)                                        as annee,
    g.date_call0_start,
    g.date_call0_end,
    g.date_call3_end,

    -- Une cohorte dont la fenetre d'appel 0 n'est pas close n'a pas encore de
    -- KPI : ses enfants sortiraient a 0 % et feraient plonger la courbe. C'est
    -- le filtre a poser avant toute lecture de la dynamique.
    (g.date_call0_end <= current_date)                              as is_call0_termine,
    -- Le cycle entier conditionne KPI_GLOBAL : une petite mission peut etre
    -- posee a n'importe lequel des quatre appels.
    (g.date_call3_end <= current_date)                              as is_cycle_acheve,

    -- KPI_GLOBAL : la petite mission comme habitude nouvelle. Numerateur.
    -- Le denominateur est l'effectif entier de la cohorte, donc count(*).
    greatest(
        coalesce(st.is_call0_goals, 0), coalesce(st.is_call1_goals, 0),
        coalesce(st.is_call2_goals, 0), coalesce(st.is_call3_goals, 0)
    )                                                               as a_recu_une_pm,

    -- Diagnostic, hors contrat : au moins une mission declaree reussie a
    -- l'appel suivant. N'entre dans aucun KPI -- le KPI global s'arrete a la
    -- pose. Deux precautions pour qui s'en sert quand meme : ce flag n'est pas
    -- emboite dans a_recu_une_pm (le suivi et la pose sont deux champs
    -- independants de l'application), et sa trajectoire est polluee par la
    -- saisie du suivi, passee de 74 a 90 % entre 2024 et 2025.
    iff(st.call1_previous_goals_follow_up = 'PM réussie'
     or st.call2_previous_goals_follow_up = 'PM réussie'
     or st.call3_previous_goals_follow_up = 'PM réussie'
     or st.call4_previous_goals_follow_up = 'PM réussie', 1, 0)     as a_declare_une_pm_reussie,

    -- KPI_1 : joint des la premiere tentative a l'appel 0.
    iff(nullif(st.nb_of_tries_call0, '') is not null, 1, 0)         as tentative_renseignee,
    iff(st.nb_of_tries_call0 = '1ère tentative', 1, 0)              as jointe_des_la_1ere,

    -- KPI_2 : appel 0 mene en entier, parmi les joignables.
    iff(nullif(st.call_0_status, '') is not null
        and st.call_0_status not in ('Numéro erroné', 'Ne pas appeler'),
        1, 0)                                                       as call0_joignable,
    iff(st.call_0_status = 'OK', 1, 0)                              as call0_ok,

    -- Dispositif de prise de RDV, finance par Fidelity. Deux leviers distincts,
    -- et c'est la distinction qui compte : la proposition est une pratique
    -- d'accompagnante (envoyer le lien Calendly), la transformation est une
    -- decision de la famille. Les suivre separement, c'est savoir lequel des
    -- deux bloque.
    coalesce(st.has_booking_proposal_call0, 0)                      as rdv_propose,
    coalesce(st.has_booking_call0, 0)                               as rdv_pris

from super_table as st
inner join groups as g
    on g.group_name = st.cohort_name
where not coalesce(st.is_excluded_from_analytics, false)
  and g.date_call0_start is not null
