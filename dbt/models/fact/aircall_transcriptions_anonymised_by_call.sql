{#
    Transcriptions Aircall anonymisees, un appel par ligne.

    Recolle en monobloc les segments de aircall_transcriptions_anonymised. C'est
    la forme dans laquelle un appel se lit : la source brute
    (MOTS_DL.MOTS_APP.aircall_calls.raw_transcription_payload) est elle aussi un
    monobloc -- un JSON par appel, 14 228 caracteres en moyenne, 301 Mo au total
    -- et le grain segment n'existe que parce que le staging l'aplatit.

    L'anonymisation, elle, reste faite au grain segment : c'est la que le
    dictionnaire sait qui parle (participant_type) et que les substitutions sont
    verifiables. Ce modele ne fait que concatener un texte deja traite.

    Le prefixe de locuteur suit la convention des requetes de diagnostic
    (d13_transcriptions_interlocuteur.sql) : "A:" pour l'accompagnante,
    "P:" pour la famille.

    /!\ La source brute reste en clair dans MOTS_DL. Ce modele est de la
    pseudonymisation pour l'analyse, pas un effacement : qui veut le second doit
    traiter raw_transcription_payload lui-meme.
#}

with segments as (
    select *
    from {{ ref('aircall_transcriptions_anonymised') }}
)

select
    call_id,
    any_value(family_id)       as family_id,
    any_value(call_session)    as call_session,
    any_value(direction)       as direction,
    any_value(answered)        as answered,
    any_value(started_at)      as started_at,
    any_value(date_started)    as date_started,
    any_value(duration)        as duration,
    any_value(caller_id)       as caller_id,
    any_value(phone_number)    as phone_number,
    any_value(asset_url)       as asset_url,

    -- La conversation remise dans l'ordre, locuteur par locuteur.
    listagg(
        iff(participant_type = 'internal', 'A: ', 'P: ') || text,
        '\n'
    ) within group (order by start_time) as transcript,

    -- Meme contenu sans les prefixes ni les tours de parole : pour les
    -- recherches plein texte et les classificateurs par mots-cles.
    listagg(text, ' ') within group (order by start_time) as transcript_brut,

    -- Le seul texte de la famille. C'est sur lui que se fonde la detection de
    -- repondeur du diagnostic d13 : un message vocal ne contient aucune parole
    -- interne.
    listagg(iff(participant_type = 'external', text, null), ' ')
        within group (order by start_time) as transcript_externe,

    count(*)                                                as nb_segments,
    count_if(participant_type = 'external')                  as nb_segments_externes,
    sum(iff(participant_type = 'external', length(text), 0)) as nb_car_externes,
    sum(regexp_count(text, '\\[[A-ZÀ-Þ_]+\\]'))              as nb_tags,

    -- Faux des que l'un des segments de l'appel attend encore la passe NER :
    -- un monobloc n'est complet que si tous ses morceaux le sont.
    booland_agg(is_anonymisation_complete) as is_anonymisation_complete,
    boolor_agg(is_sans_dictionnaire)       as has_segment_sans_dictionnaire

from segments
group by call_id
