/*
    Verrouille le comportement des macros d'anonymisation (macros/anonymise.sql).

    Chaque cas ci-dessous correspond a un bug reellement rencontre en
    construisant aircall_transcriptions_dict. Ils sont peu intuitifs et aucun ne
    se voit sur un echantillon : ils ne se manifestent que par un nom laisse en
    clair quelque part dans 2 millions de segments. D'ou ce test.

    Le test echoue en renvoyant les cas dont le nombre de remplacements
    s'ecarte de l'attendu.
*/

with cas as (
    -- (1) La reconnaissance vocale ecrit les prenoms sans accent une fois sur
    -- deux : le motif doit attraper les deux graphies.
    select 'accents, deux graphies'        as cas,
           'Rachël'                        as nom,
           'Rachel et Rachël'              as txt,
           2                               as attendu

    -- (2) Initiale accentuee. Deux bugs se cachaient ici : lower() applique
    -- apres translate() (la liste d'accents est en minuscules), et le flag 'i'
    -- de Snowflake qui ne replie pas la casse des accentues.
    union all select 'initiale accentuee', 'Élise', 'Bonjour Élise, ici Elise.', 2

    -- (3) Majuscules integrales : fin de segment criee ou sigle.
    union all select 'tout en majuscules', 'Léa', 'LÉA et Lea et Léa', 3

    -- (4) Un prenom ne doit pas redacter les mots qui le contiennent.
    union all select 'pas de sous-chaine', 'Léa', 'Léandre et Oléa et Léana', 0
    union all select 'pas de sous-chaine', 'Rachël', 'Rachelle est partie', 0
    union all select 'pas de sous-chaine', 'Élise', 'Élisabeth a repondu', 0

    -- (5) Noms adjacents. La frontiere de fin etant consommee par le match, le
    -- second nom etait perdu sans la seconde passe. Mesure a l'epoque :
    -- 2 043 occurrences d'accompagnantes manquantes sur le corpus.
    union all select 'noms adjacents', 'Marie', 'Marie Marie Marie', 3

    -- (6) Un tag deja pose ne doit jamais etre re-tague. Les crochets etant
    -- des non-lettres, "ACCOMPAGNANTE" dans "[ACCOMPAGNANTE]" formait un mot
    -- valide : deux comptes admin_users portent ce mot dans leur nom et
    -- 22 977 segments ressortaient en "[[[ACCOMPAGNANTE]]]".
    union all select 'tag deja pose', 'accompagnante',
                     'Oui bonjour, c est [ACCOMPAGNANTE] de l association', 0

    -- (7) Bords de chaine : un segment de parole commence et finit souvent par
    -- le prenom de l'interlocuteur ("Omar ? ... au revoir Omar").
    union all select 'bords de chaine', 'Omar', 'Omar en tete, a la fin Omar', 2
),

-- (8) Cas particulier : un fragment ne doit pas matcher derriere une lettre
-- accentuee. C'est la regression que produisait \b, ASCII-only, pour qui le
-- "é" de "Aurélie" est un non-mot et donc une frontiere valide : le motif
-- "lie" y matchait et "Aurélie" ressortait "Auré[TAG]".
fragment as (
    select 'fragment derriere accent' as cas,
           'Aurélie a rappele'        as txt,
           'l[iîïIÎÏ][eéèêëEÉÈÊË]'    as cls,
           0                          as attendu
),

resultats as (
    select cas, txt, attendu,
           regexp_count({{ scrub('txt', 'cls', '@@') }}, '@@') as obtenu
    from (
        select cas, nom, txt, attendu, {{ accent_class('nom') }} as cls
        from cas
    )

    union all
    select cas, txt, attendu,
           regexp_count({{ scrub('txt', 'cls', '@@') }}, '@@') as obtenu
    from fragment

    -- (9) Motif nul (famille sans aucun nom connu) : le texte ressort intact.
    union all
    select 'motif nul' as cas, txt, 0 as attendu,
           regexp_count({{ scrub('txt', 'cls', '@@') }}, '@@') as obtenu
    from (select 'Bonjour Rachel' as txt, null::string as cls)

    -- (10) Identifiants directs reperables sans dictionnaire.
    union all
    select 'telephone' as cas, txt, attendu,
           regexp_count({{ scrub_contacts('txt') }}, '\\[TELEPHONE\\]') as obtenu
    from (
        select '06 12 34 56 78' as txt, 1 as attendu
        union all select 'appelez le 0612345678', 1
        union all select '+33 6 12 34 56 78', 1
        union all select 'il a 6 ans et 12 mois', 0
    )

    union all
    select 'email' as cas, txt, attendu,
           regexp_count({{ scrub_contacts('txt') }}, '\\[EMAIL\\]') as obtenu
    from (select 'ecrivez a parent@example.com' as txt, 1 as attendu)
)

select cas, txt, attendu, obtenu
from resultats
where obtenu != attendu
