with source as (
    select *
    from {{ source('mots_app', 'child_supports') }}
),

source2 as (
    select 
        *,
        parse_json(object) as object_json,
        parse_json(object_changes) as object_json_changes
    from {{ source('mots_app', 'versions') }}
),

filterr as (
    select
        id,
        item_id,
        row_number() over (partition by item_id order by created_at desc) as rn
    from source2
    where item_type = 'ChildSupport'
    qualify rn = 1
)

-- Il faut intégrer les données du object_json_changes dans le object_json
select
    coalesce(source.id::string, object_json:id::string) as family_id,
    --source.supporter_id::string as supporter_id,
    coalesce(object_json:supporter_id::string, source.supporter_id::string) as supporter_id,
    -- Call 0 fields
    coalesce(object_json:call0_attempt::string, source.call0_attempt::string) as call0_attempt,
    coalesce(object_json:call0_duration::integer, source.call0_duration::integer) as call0_duration,
    coalesce(nullif(trim(rtrim(object_json:call0_status::string)), ''), source.call0_status::string) as call0_status,
    coalesce(object_json:call0_review::string, source.call0_review::string) as call0_review,
    coalesce(object_json:call0_goals_sms::string, source.call0_goals_sms::string) as call0_goals_sms,
    coalesce(nullif(object_json:call0_goals::string, ''), source.call0_goals::string) as call0_goals,
    coalesce(object_json:call0_sendings_benefits_details::string, source.call0_sendings_benefits_details::string) as call0_sendings_benefits_details,
    coalesce(object_json:call0_talk_needed::boolean, source.call0_talk_needed::boolean) as call0_talk_needed,
    coalesce(object_json:call0_why_talk_needed::string, source.call0_why_talk_needed::string) as call0_why_talk_needed,
    -- Call 1 fields
    coalesce(object_json:call1_attempt::string, source.call1_attempt::string) as call1_attempt,
    coalesce(object_json:call1_duration::integer, source.call1_duration::integer) as call1_duration,
    coalesce(nullif(trim(rtrim(object_json:call1_status::string)), ''), source.call1_status::string) as call1_status,
    coalesce(object_json:call1_review::string, source.call1_review::string) as call1_review,
    coalesce(object_json:call1_goals_sms::string, source.call1_goals_sms::string) as call1_goals_sms,
    coalesce(nullif(object_json:call1_goals::string, ''), source.call1_goals::string) as call1_goals,
    coalesce(object_json:call1_sendings_benefits_details::string, source.call1_sendings_benefits_details::string) as call1_sendings_benefits_details,
    coalesce(object_json:call1_previous_goals_follow_up::string, source.call1_previous_goals_follow_up::string) as call1_previous_goals_follow_up,
    coalesce(object_json:call1_talk_needed::boolean, source.call1_talk_needed::boolean) as call1_talk_needed,
    coalesce(object_json:call1_why_talk_needed::string, source.call1_why_talk_needed::string) as call1_why_talk_needed,
    -- Call 2 fields
    coalesce(object_json:call2_attempt::string, source.call2_attempt::string) as call2_attempt,
    coalesce(object_json:call2_duration::integer, source.call2_duration::integer) as call2_duration,
    coalesce(nullif(trim(rtrim(object_json:call2_status::string)), ''), source.call2_status::string) as call2_status,
    coalesce(object_json:call2_review::string, source.call2_review::string) as call2_review,
    coalesce(object_json:call2_goals_sms::string, source.call2_goals_sms::string) as call2_goals_sms,
    coalesce(nullif(object_json:call2_goals::string, ''), source.call2_goals::string) as call2_goals,
    coalesce(object_json:call2_sendings_benefits_details::string, source.call2_sendings_benefits_details::string) as call2_sendings_benefits_details,
    coalesce(object_json:call2_previous_goals_follow_up::string, source.call2_previous_goals_follow_up::string) as call2_previous_goals_follow_up,
    coalesce(object_json:call2_talk_needed::boolean, source.call2_talk_needed::boolean) as call2_talk_needed,
    coalesce(object_json:call2_why_talk_needed::string, source.call2_why_talk_needed::string) as call2_why_talk_needed,
    -- Call 3 fields
    coalesce(object_json:call3_attempt::string, source.call3_attempt::string) as call3_attempt,
    coalesce(object_json:call3_duration::integer, source.call3_duration::integer) as call3_duration,
    coalesce(nullif(trim(rtrim(object_json:call3_status::string)), ''), source.call3_status::string) as call3_status,
    coalesce(object_json:call3_review::string, source.call3_review::string) as call3_review,
    coalesce(object_json:call3_goals_sms::string, source.call3_goals_sms::string) as call3_goals_sms,
    coalesce(nullif(object_json:call3_goals::string, ''), source.call3_goals::string) as call3_goals,
    coalesce(object_json:call3_sendings_benefits_details::string, source.call3_sendings_benefits_details::string) as call3_sendings_benefits_details,
    coalesce(object_json:call3_previous_goals_follow_up::string, source.call3_previous_goals_follow_up::string) as call3_previous_goals_follow_up,
    coalesce(object_json:call3_talk_needed::boolean, source.call3_talk_needed::boolean) as call3_talk_needed,
    coalesce(object_json:call3_why_talk_needed::string, source.call3_why_talk_needed::string) as call3_why_talk_needed,
    -- Call 4 fields
    coalesce(object_json:call4_previous_goals_follow_up::string, source.call4_previous_goals_follow_up::string) as call4_previous_goals_follow_up,
    -- Timestamps
    coalesce(to_date(nullif(object_json:created_at::string, '')), to_date(nullif(source.created_at::string, ''))) as date_created,
    coalesce(to_date(nullif(object_json:updated_at::string, '')), to_date(nullif(source.updated_at::string, ''))) as date_updated,
    coalesce(object_json:is_bilingual::string, source.is_bilingual::string) as is_bilingual,
    -- Données du formulaire d'inscription, collectées depuis le 2026-03-27.
    -- Elles sont lues directement dans source, sans passer par object_json :
    -- le `object` de PaperTrail contient l'état AVANT la dernière modification,
    -- donc le coalesce utilisé pour les autres colonnes renverrait un tableau
    -- vide pour toute famille modifiée après avoir rempli le formulaire
    -- (345 des 1 545 familles répondantes au 2026-09-29).
    -- La source est toujours au moins aussi fraîche : aucun cas inverse constaté.
    source.enrollment_reasons as enrollment_reasons,
    -- Même problème sur is_bilingual, qui existe pourtant de longue date :
    -- la colonne historique ci-dessus est périmée pour 345 des 2 746 familles
    -- inscrites depuis le 2026-03-27, et sous-estime le taux de remplissage
    -- (1 608 renseignées au lieu de 1 953). Elle est laissée telle quelle pour
    -- ne pas déplacer les chiffres des modèles existants ; is_bilingual_current
    -- est la valeur à jour, à utiliser pour toute nouvelle analyse.
    source.is_bilingual::string as is_bilingual_current,
    nullif(trim(source.second_language::string), '') as second_language,
    coalesce(object_json:module2_chosen_by_parents_id::integer, source.module2_chosen_by_parents_id::integer) as module2_chosen_by_parents_id,
    coalesce(object_json:module3_chosen_by_parents_id::integer, source.module3_chosen_by_parents_id::integer) as module3_chosen_by_parents_id,
    coalesce(object_json:module4_chosen_by_parents_id::integer, source.module4_chosen_by_parents_id::integer) as module4_chosen_by_parents_id,
    coalesce(object_json:module5_chosen_by_parents_id::integer, source.module5_chosen_by_parents_id::integer) as module5_chosen_by_parents_id,
    coalesce(object_json:module6_chosen_by_parents_id::integer, source.module6_chosen_by_parents_id::integer) as module6_chosen_by_parents_id,
    nullif(replace(notes::string, ' ', ''), '') as notes,
    nullif(stop_support_reason::string, '') as stop_support_reason,
     to_date(
        nullif(stop_support_date::string, '')
    ) as date_stop_support
from source
left join source2
    on source.id = source2.item_id
left join filterr
    on source2.id = filterr.id
where (source2.item_id is null or filterr.rn = 1)