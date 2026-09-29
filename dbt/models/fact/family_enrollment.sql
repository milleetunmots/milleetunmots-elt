-- Une ligne par famille : ce que le formulaire d'inscription déclare sur ses
-- motivations et son multilinguisme.
--
-- Trois pièges à connaître avant de brancher la BI dessus :
--
-- 1. Les raisons d'inscription ne sont collectées que depuis le 2026-03-27.
--    Tout taux de remplissage doit filtrer sur is_after_enrollment_form_launch,
--    sinon le dénominateur inclut quatre ans d'inscriptions pour lesquelles la
--    question n'existait pas.
-- 2. is_bilingual, lui, existe depuis 2022 : ce n'est pas une nouvelle donnée.
--    Il est renseigné soit au formulaire, soit plus tard par l'accompagnante
--    au call 0 (625 des 1 951 familles renseignées depuis le 2026-03-27).
--    Les deux modes de saisie sont mélangés, et le taux de remplissage des
--    inscriptions récentes continue de monter tant que les call 0 se font.
-- 3. Les deux questions sont facultatives : ~56 % de remplissage sur les
--    raisons, ~71 % sur le multilinguisme.

with child_supports as (
    select *
    from {{ ref('stg_1001mots_app__child_supports') }}
),

children as (
    select *
    from {{ ref('child') }}
),

groups as (
    select *
    from {{ ref('groups') }}
),

-- 3 827 familles sur 32 659 comptent plusieurs enfants. On rattache la famille
-- au cadet, comme super_table, pour que l'âge à l'inscription soit celui de
-- l'enfant que le programme accompagne.
youngest_child as (
    select *
    from children
    where date_discarded is null
    qualify row_number() over (
        partition by family_id
        order by date_birth desc, child_id
    ) = 1
)

select
    cs.family_id,
    cs.supporter_id,
    yc.child_id as youngest_child_id,
    g.group_name as cohort_name,

    -- Inscription
    cs.date_created as date_of_registration,
    date_trunc('month', cs.date_created) as month_of_registration,
    cs.date_created >= {{ var('enrollment_form_start_date') }} as is_after_enrollment_form_launch,

    -- Âge de l'enfant à l'inscription
    yc.date_birth,
    yc.age_at_registration as age_at_registration_in_months,
    -- Classification historique du projet (macro compute_age_in_month), reprise
    -- telle quelle pour rester comparable aux dashboards existants. Pour une
    -- lecture en trois paliers, regrouper A+B / C+D / E+F — en gardant en tête
    -- que la borne de B est "<= 12 mois", donc 12 mois pile tombe dans B.
    yc.child_age_in_month as age_bucket_at_registration,

    -- Raisons d'inscription (choix multiples, facultatif).
    -- Le détail option par option est dans family_enrollment_reasons :
    -- une ligne par couple (famille, raison).
    cs.enrollment_reasons,
    coalesce(array_size(cs.enrollment_reasons), 0) as nb_enrollment_reasons,
    coalesce(array_size(cs.enrollment_reasons), 0) > 0 as has_enrollment_reasons,

    -- Multilinguisme
    -- is_bilingual_current, et non is_bilingual : la colonne historique est
    -- périmée pour 345 familles depuis le 2026-03-27 (voir le staging).
    cs.is_bilingual_current as is_bilingual,
    cs.is_bilingual_current in ('0_yes', '1_no') as is_multilingual_known,
    case
        when cs.is_bilingual_current = '0_yes' then true
        when cs.is_bilingual_current = '1_no' then false
        else null
    end as is_multilingual,

    -- Langue déclarée : champ libre, 720 libellés distincts pour 1 198 réponses
    -- même après passage en minuscules. À regrouper avant tout usage en BI
    -- ("arabe", "arabe marocain", "arabe algérien"... sont comptés séparément).
    cs.second_language,
    lower(cs.second_language) as second_language_clean

from child_supports cs
left join youngest_child yc
    on yc.family_id = cs.family_id
left join groups g
    on g.group_id = yc.group_id
