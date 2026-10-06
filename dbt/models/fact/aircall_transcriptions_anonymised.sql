{#
    Transcriptions Aircall anonymisees -- modele de sortie.

    Reunit les deux passes :
      1. fact.aircall_transcriptions_dict, le dictionnaire (parent, fratrie,
         accompagnante, telephones) applique a tous les segments dans
         l'entrepot ;
      2. fact.aircall_transcriptions_ner, les tiers et adresses detectes par
         GLiNER, produit par admin/anonymize_aircall_ner.py.

    La jointure se fait sur le hash du texte, pas sur le segment. Les 117 000
    segments candidats ne portent que 93 000 textes distincts -- les formules de
    politesse et les messages de repondeur se repetent d'un appel a l'autre --
    donc la table NER est vingt fois plus petite que le corpus et une relance
    ne recalcule que les textes jamais vus.

    C'est licite parce que le NER tourne APRES le dictionnaire : deux segments
    de meme texte post-dictionnaire sont interchangeables pour lui, la part
    propre a la famille ayant deja ete substituee.

    /!\ is_anonymisation_complete dit si le segment a vu les DEUX passes. Un
    segment candidat dont le texte n'est pas encore dans la table NER ressort
    avec le texte du dictionnaire seul -- correct mais incomplet. Filtrer
    dessus pour tout usage ou l'exhaustivite compte.
#}

with dict_pass as (
    select *
    from {{ ref('aircall_transcriptions_dict') }}
),

ner_pass as (
    select
        text_hash,
        text_ner
    from {{ source('fact_anonymised', 'aircall_transcriptions_ner') }}
    qualify row_number() over (partition by text_hash order by date_traite desc) = 1
)

select
    d.call_id,
    d.family_id,
    d.start_time,
    d.end_time,
    d.participant_type,
    d.user_id,
    d.direction,
    d.answered,
    d.started_at,
    d.date_started,
    d.call_session,
    d.duration,
    d.caller_id,
    d.phone_number,
    d.asset_url,
    d.tags,

    coalesce(n.text_ner, d.text_dict) as text,

    d.is_candidat_ner,
    (not d.is_candidat_ner or n.text_hash is not null) as is_anonymisation_complete,
    d.is_sans_dictionnaire

from dict_pass as d
left join ner_pass as n
    on n.text_hash = md5(d.text_dict)
