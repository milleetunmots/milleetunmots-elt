{#
    Passe 1 sur 2 de l'anonymisation des transcriptions Aircall : le dictionnaire.

    Grain : un segment de parole (~2 M lignes). C'est l'etage lourd, il tourne
    donc entierement dans l'entrepot -- aucun aller-retour en Python sur 103 Mo
    de texte.

    Le principe qui rend l'exercice tractable ici, contrairement aux fichiers
    .docx de admin/camille/ : les transcriptions sont jointes a la base, donc on
    connait NOMMEMENT les personnes au bout du fil (parent, enfant,
    accompagnante) via child_support_id. 99,9 % des segments se raccrochent a un
    dictionnaire d'identites. Le NER (passe 2) ne sert plus qu'aux tiers cites
    en passant -- la grand-mere, le medecin, une commune.

    Choix assume : on SUR-redacte plutot que l'inverse. Si un enfant s'appelle
    Rose, toutes les "rose" du texte sautent. On n'exclut donc aucun prenom qui
    collisionne avec un mot courant : un faux positif coute de la lisibilite,
    un faux negatif coute une reidentification.

    Conserve en clair, par decision (cf. git log) : asset_url, caller_id,
    phone_number. Seul le texte est traite. La table reste donc
    reidentifiable par l'enregistrement audio -- elle n'a pas vocation a sortir
    du meme perimetre d'acces que la source.
#}

with transcriptions as (
    select *
    from {{ ref('stg_1001mots_app__aircall_call_transcriptions') }}
),

children as (
    select *
    from {{ ref('stg_1001mots_app__children') }}
),

parents as (
    select *
    from {{ ref('stg_1001mots_app__parents') }}
),

child_supports as (
    select *
    from {{ ref('stg_1001mots_app__child_supports') }}
),

admin_users as (
    select *
    from {{ ref('stg_1001mots_app__admin_users') }}
),

-- Un nom connu par ligne, avec son role. Les quatre sources : les deux parents,
-- la fratrie (plusieurs enfants par famille) et l'accompagnante attitree.
noms_bruts as (
    select c.family_id, p.first_name as nom, 'parent' as role
    from children as c
    join parents as p on p.parent_id = c.parent1_id::int

    union all
    select c.family_id, p.last_name, 'parent'
    from children as c
    join parents as p on p.parent_id = c.parent1_id::int

    union all
    select c.family_id, p.first_name, 'parent'
    from children as c
    join parents as p on p.parent_id = c.parent2_id::int

    union all
    select c.family_id, p.last_name, 'parent'
    from children as c
    join parents as p on p.parent_id = c.parent2_id::int

    union all
    select c.family_id, c.first_name, 'enfant'
    from children as c

    union all
    select c.family_id, c.last_name, 'enfant'
    from children as c

    union all
    select cs.family_id, au.name, 'accompagnante'
    from child_supports as cs
    join admin_users as au on au.supporter_id = cs.supporter_id
),

-- Les noms composes doivent matcher aussi en morceaux : "Marie-Claire" est
-- souvent transcrit "Marie" tout court. On garde donc le nom entier ET ses
-- tokens, et on les ordonnera du plus long au plus court (l'alternation regex
-- est leftmost-first : sans cet ordre, "Marie" consommerait "Marie-Claire").
noms_tokens as (
    select family_id, role, trim(nom) as tok
    from noms_bruts
    where nom is not null

    union
    select family_id, role, trim(f.value::string) as tok
    from noms_bruts,
        lateral flatten(input => split(replace(replace(nom, '-', ' '), '''', ' '), ' ')) as f
    where nom is not null
),

noms_filtres as (
    select
        family_id,
        role,
        tok,
        length(tok) as tok_len,
        {{ accent_class('tok') }} as cls
    from noms_tokens
    -- en dessous de 3 caracteres un nom genere trop de faux positifs ("Ali"
    -- passe, "Li" non) ; on ecarte aussi les tokens non alphabetiques
    where length(trim(tok)) >= 3
      and trim(tok) rlike '[A-Za-zÀ-ÿ].*'
),

-- Une alternation regex par role et par famille.
patterns as (
    select
        family_id,
        nullif(listagg(iff(role = 'parent', cls, null), '|')
               within group (order by tok_len desc), '')        as pattern_parent,
        nullif(listagg(iff(role = 'enfant', cls, null), '|')
               within group (order by tok_len desc), '')        as pattern_enfant,
        nullif(listagg(iff(role = 'accompagnante', cls, null), '|')
               within group (order by tok_len desc), '')        as pattern_accompagnante
    from noms_filtres
    group by family_id
),

-- Les accompagnantes se citent entre elles ("je passe le dossier a Severine").
-- Une liste globale du personnel rattrape ces mentions hors famille. C'est la
-- raison pour laquelle Marie, Claire et Severine sont dans le top 30 des mots
-- capitalises du corpus.
pattern_equipe as (
    select nullif(listagg(cls, '|') within group (order by tok_len desc), '') as pattern_staff
    from (
        select distinct tok, length(tok) as tok_len, {{ accent_class('tok') }} as cls
        from (
            select trim(f.value::string) as tok
            from admin_users,
                lateral flatten(input => split(replace(name, '-', ' '), ' ')) as f
            where name is not null
        )
        where length(tok) >= 3 and tok rlike '[A-Za-zÀ-ÿ].*'
    )
),

scrubbed as (
    select
        t.call_id,
        t.child_support_id as family_id,
        t.start_time,
        t.end_time,
        t.participant_type,
        t.user_id,
        t.direction,
        t.answered,
        t.started_at,
        t.date_started,
        t.call_session,
        t.duration,
        t.caller_id,
        t.phone_number,
        t.asset_url,
        t.tags,

        -- Enchainement des passes. L'ordre compte peu entre roles (un nom
        -- partage part sous le premier tag rencontre), mais les contacts
        -- passent en dernier : une suite de chiffres ne peut plus etre masquee
        -- par un tag de nom.
        {{ scrub(
             scrub(
               scrub(
                 scrub(
                   scrub_contacts('t.text'),
                   'p.pattern_parent', '[PARENT]'),
                 'p.pattern_enfant', '[ENFANT]'),
               'p.pattern_accompagnante', '[ACCOMPAGNANTE]'),
             'e.pattern_staff', '[ACCOMPAGNANTE]')
        }} as text_dict,

        (p.family_id is null) as is_sans_dictionnaire

    from transcriptions as t
    left join patterns as p on p.family_id = t.child_support_id::string
    cross join pattern_equipe as e
    where t.text is not null
)

select
    *,
    {{ is_ner_candidate('text_dict') }} as is_candidat_ner
from scrubbed
