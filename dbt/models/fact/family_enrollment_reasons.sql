-- Une ligne par couple (famille, raison d'inscription cochée).
--
-- Table de liaison : c'est la forme qu'attend un outil de BI pour une question
-- à choix multiples. Les familles cochent 2,6 raisons en moyenne, donc les
-- pourcentages par raison ne somment pas à 100 %.
--
-- Pour un taux de réponse par raison, le dénominateur est le nombre de familles
-- répondantes, pas le nombre de lignes :
--   count(distinct family_id) ici
--   / count(distinct family_id) dans family_enrollment
--     where has_enrollment_reasons and is_after_enrollment_form_launch
-- en filtrant des deux côtés sur is_after_enrollment_form_launch.
--
-- Compter en count(distinct family_id) et non en count(*) : une famille pourrait
-- cocher deux libellés qui se ramènent au même label (aucun cas au 2026-09-29).

with family_enrollment as (
    select *
    from {{ ref('family_enrollment') }}
),

exploded as (
    select
        fe.family_id,
        fe.cohort_name,
        fe.date_of_registration,
        fe.month_of_registration,
        fe.is_after_enrollment_form_launch,
        fe.age_at_registration_in_months,
        fe.age_bucket_at_registration,
        fe.is_multilingual,
        fe.nb_enrollment_reasons,
        r.index::integer as reason_position,
        r.value::string as reason_raw
    from family_enrollment as fe,
    lateral flatten(input => fe.enrollment_reasons) as r
),

-- Le formulaire reformule ses libellés selon l'âge de l'enfant ("mon bébé" /
-- "mon enfant", "préparer son entrée à l'école (jamais trop tôt !)"), donc le
-- libellé brut éclate les volumes sur plusieurs lignes. Les motifs utilisent %
-- à la place des apostrophes, présentes en version droite (') et typographique (’).
-- Le préfixe de tri reprend l'ordre de fréquence observé au 2026-09-29.
-- Tout ce qui n'est pas reconnu vient du champ libre "Autre".
labelled as (
    select
        exploded.*,
        case
            when reason_raw ilike 'Pour avoir des idées d%activités%'                    then 'A/ Idées d''activités avec mon enfant'
            when reason_raw ilike 'Pour l%occuper sans les écrans%'                      then 'B/ L''occuper sans écrans'
            when reason_raw ilike 'Parce que c%est mon premier%'                         then 'C/ C''est mon premier enfant'
            when reason_raw ilike 'Pour recevoir des livres gratuits%'                   then 'D/ Recevoir des livres gratuits'
            when reason_raw ilike 'Pour préparer son entrée à l%école%'                  then 'E/ Préparer l''entrée à l''école'
            when reason_raw ilike 'Parce que le français n%est pas ma langue maternelle%' then 'F/ Le français n''est pas ma langue maternelle'
            when reason_raw ilike 'Je suis inquiet(e) pour son langage%'                 then 'G/ Inquiétude sur son langage'
            when reason_raw ilike 'J%ai un autre enfant qui a parlé tard%'               then 'H/ Un autre enfant a parlé tard'
            when reason_raw ilike 'Parce que je suis peu entouré(e)%'                    then 'I/ Peu entouré(e) pour élever mon enfant'
            else 'J/ Autre (réponse libre)'
        end as reason_label
    from exploded
)

select
    labelled.*,
    reason_label = 'J/ Autre (réponse libre)' as is_free_text_reason
from labelled
