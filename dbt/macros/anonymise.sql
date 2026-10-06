{#
    Briques d'anonymisation du texte libre.

    La difficulte : on connait nommement les personnes (parent, enfant,
    accompagnante) mais la transcription Aircall est produite par une
    reconnaissance vocale qui ecrit les prenoms sans accent une fois sur deux
    ("Rachel" pour "Rachël", "Lea" pour "Léa"). Un remplacement litteral laisse
    donc passer la moitie des occurrences.

    D'ou `accent_class` : on retire les accents du nom connu, puis on reouvre
    chaque voyelle en classe de caracteres. "Rachël" devient le motif
    r[aàâä][cç]h[eéèêë]l, qui attrape les deux graphies.

    Les motifs sont ancres sur \b : "Léa" ne doit pas redacter "Léandre" ni
    "Oléa". Teste sur ces trois cas precis.
#}

{% macro accent_class(col) %}
    replace(replace(replace(replace(replace(replace(
        -- lower() AVANT translate() : la liste d'accents ci-dessous est en
        -- minuscules, donc un prenom a initiale accentuee ("Elise" ecrit "Élise")
        -- ressortirait non deaccentue et son motif ne matcherait que la graphie
        -- accentuee -- or la reconnaissance vocale ecrit "elise" une fois sur deux.
        translate(
            lower(regexp_replace({{ col }}, '[^A-Za-zÀ-ÿ \-'']', '')),
            'àâäéèêëîïôöùûüç', 'aaaeeeeiioouuuc'
        ),
        -- Les deux casses sont listees explicitement : le flag 'i' de Snowflake
        -- ne replie pas la casse des caracteres accentues ("É" n'y est pas
        -- reconnu comme equivalent de "é"). Sans cela tout prenom a initiale
        -- accentuee echappe au dictionnaire -- c'est ce qui laissait 550
        -- occurrences d'un meme prenom en clair.
        'a', '[aàâäAÀÂÄ]'), 'e', '[eéèêëEÉÈÊË]'), 'i', '[iîïIÎÏ]'),
        'o', '[oôöOÔÖ]'), 'u', '[uùûüUÙÛÜ]'), 'c', '[cçCÇ]')
{% endmacro %}


{#
    Remplace dans `text_col` toutes les occurrences du motif `pattern_col`
    (alternation deja construite) par `tag`. Insensible a la casse.
    Si le motif est nul (famille sans nom connu), le texte ressort intact.
#}
{% macro scrub(text_col, pattern_col, tag) %}
    iff(
        {{ pattern_col }} is null,
        {{ text_col }},
        -- Frontieres de mot ecrites a la main plutot que \b : celui de Snowflake
        -- est ASCII-only, donc " Élise" n'offre aucune frontiere detectable
        -- (le "É" y compte comme non-mot) et le prenom echappe au motif.
        -- Les deux frontieres sont capturees puis reemises (\1 et \3).
        --
        -- Les crochets sont exclus des frontieres, et ce n'est pas cosmetique :
        -- sans cela, "ACCOMPAGNANTE" dans un tag deja pose est un mot valide
        -- delimite par "[" et "]". Deux comptes admin_users portent ce mot dans
        -- leur nom, et le tag se faisait re-taguer a chaque passe --
        -- 22 977 segments ressortaient en "[[[ACCOMPAGNANTE]]]".
        --
        -- Deux passes, et ce n'est pas de la precaution : la frontiere de fin
        -- etant consommee par le match, deux noms cote a cote perdent le second
        -- ("Marie Claire" ne taguait que Marie, l'espace ayant ete absorbe).
        -- La passe 1 reemet cette frontiere, la passe 2 la trouve. Mesure a
        -- l'appui : sans elle, 2 043 occurrences d'accompagnantes manquaient.
        regexp_replace(
            regexp_replace(
                {{ text_col }},
                '(^|[^A-Za-zÀ-ÿ0-9_\\[\\]])(' || {{ pattern_col }} || ')([^A-Za-zÀ-ÿ0-9_\\[\\]]|$)',
                '\\1{{ tag }}\\3', 1, 0, 'i'
            ),
            '(^|[^A-Za-zÀ-ÿ0-9_\\[\\]])(' || {{ pattern_col }} || ')([^A-Za-zÀ-ÿ0-9_\\[\\]]|$)',
            '\\1{{ tag }}\\3', 1, 0, 'i'
        )
    )
{% endmacro %}


{#
    Identifiants directs reperables sans dictionnaire.
    Les numeros dictes a l'oral sont transcrits en chiffres par Aircall
    ("zero six douze" -> "06 12"), d'ou la tolerance aux separateurs.
#}
{% macro scrub_contacts(text_col) %}
    regexp_replace(
        regexp_replace(
            {{ text_col }},
            '[A-Za-z0-9._%+\\-]+@[A-Za-z0-9.\\-]+\\.[A-Za-z]{2,}',
            '[EMAIL]', 1, 0, 'i'
        ),
        '(\\+33|0033|0)[\\s.\\-]?[1-9]([\\s.\\-]?[0-9]{2}){4}',
        '[TELEPHONE]', 1, 0, 'i'
    )
{% endmacro %}


{#
    Vocabulaire que l'anonymisation ne doit jamais prendre pour un nom propre.

    La liste vit dans dbt_project.yml (var anonymise_stopwords) et non ici,
    parce que admin/anonymize_aircall_ner.py doit lire exactement la meme. Quand
    chaque cote en gardait sa copie, elles ont derive : GLiNER s'est mis a
    taguer "Allo" -- 3 686 segments -- comme un prenom, ce qui aurait casse le
    classificateur de repondeurs du diagnostic d13.
#}
{% macro anonymise_stopwords() %}
{{- var('anonymise_stopwords') -}}
{% endmacro %}


{#
    Faut-il passer ce segment au NER ?

    Deux declencheurs. Un mot capitalise que ni le dictionnaire ni la stop-list
    n'ont consomme -- c'est le cas general, il ramene le corpus de 103 Mo a
    environ 20 Mo. Et la presence d'un mot de voie, parce qu'une adresse postale
    est la seule PII que le dictionnaire ne peut par construction pas connaitre :
    elle n'est pas en base.
#}
{% macro is_ner_candidate(text_col) %}
    (
        -- Meme precaution que dans scrub() : des frontieres explicites, pas \b.
        -- Avec \b, aucun mot accentue de la stop-list n'etait retire -- entre le
        -- "ô" de "allô" et la virgule suivante il n'y a pas de frontiere ASCII --
        -- et "Allô" suffisait a classer le segment candidat au NER.
        regexp_like(
            regexp_replace(
                regexp_replace(
                    {{ text_col }},
                    '(^|[^A-Za-zÀ-ÿ0-9_])(' || '{{ anonymise_stopwords() }}' || ')([^A-Za-zÀ-ÿ0-9_]|$)',
                    '\\1 \\3', 1, 0, 'i'
                ),
                '(^|[^A-Za-zÀ-ÿ0-9_])(' || '{{ anonymise_stopwords() }}' || ')([^A-Za-zÀ-ÿ0-9_]|$)',
                '\\1 \\3', 1, 0, 'i'
            ),
            '.*[A-ZÀ-Þ][a-zà-þ]{2,}.*'
        )
        or regexp_like(
            {{ text_col }},
            '.*\\b(rue|avenue|boulevard|impasse|all[ée]e|chemin|route|r[ée]sidence|lotissement)\\b.*',
            'i'
        )
    )
{% endmacro %}
