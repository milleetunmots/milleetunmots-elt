-- Une ligne par couple (famille, langue parlée à la maison).
--
-- Le champ source, child_supports.second_language, est un texte libre renseigné
-- depuis 2019 : 10 512 familles, 3 091 orthographes distinctes. Il contient des
-- listes ("arabe, français", "lingala et français"), des annotations
-- ("arabe (papa)"), des pays à la place des langues ("sénégalais", "mali"), des
-- fautes de frappe ("soniké", "frçs") et des non-réponses ("papa", "?").
--
-- Ce modèle le découpe, le nettoie et le rattache à language_mapping, qui porte
-- la table de correspondance variante -> langue. Pour corriger ou compléter un
-- regroupement, éditer cette table, pas ce fichier.
--
-- Elle est chargée par `dbt seed` depuis dbt/seeds/language_mapping.csv, qui
-- reste hors du dépôt (.gitignore exclut tout *.csv). On l'appelle donc par
-- source() et non par ref() : avec ref(), tout environnement qui part du dépôt
-- -- dbt Cloud, CI, un clone -- casse au parsing, le nœud étant introuvable.
-- Corriger un regroupement se fait donc en deux temps : éditer le CSV local,
-- puis `dbt seed --select language_mapping`.
--
-- Couverture au 2026-10-02 : 96 % des tokens sont reconnus. Le reste ressort
-- avec language_label à null et is_unmapped à true, le token brut conservé dans
-- language_token pour pouvoir enrichir le seed.

with family_enrollment as (
    select *
    from {{ ref('family_enrollment') }}
),

language_mapping as (
    select *
    from {{ source('seed', 'language_mapping') }}
),

-- Les séparateurs sont hétérogènes : virgule, "et", tiret entouré d'espaces,
-- barre oblique. Les parenthèses sont retirées d'abord, elles ne contiennent
-- que des précisions ("(papa)", "(mali)") qui feraient du bruit.
separated as (
    select
        family_id,
        regexp_replace(
          regexp_replace(
            regexp_replace(lower(trim(second_language)), '\\([^)]*\\)', ' '),
            '[[:space:]]+(et|and|avec|ou|puis)[[:space:]]+', ','),
          '([[:space:]]+-[[:space:]]*)|([[:space:]]*-[[:space:]]+)', ',') as prepared
    from family_enrollment
    where second_language is not null
),

tokenised as (
    select family_id, trim(t.value::string) as token
    from separated,
    lateral flatten(input => strtok_to_array(prepared, ',;/&+|')) as t
),

-- Articles et atténuations en tête ou en queue de token ("le français",
-- "un peu d'anglais", "arabe uniquement"). Le rognage final ne retire que de la
-- ponctuation : [[:alpha:]] est ASCII-only dans Snowflake et mangerait les
-- accents de fin, transformant "soninké" en "sonink".
cleaned as (
    select
        family_id,
        trim(regexp_replace(regexp_replace(regexp_replace(
            token,
            '^(le |la |les |l''|l’|du |de la |de |des |un peu d''|un peu de |un peu |que |uniquement |autre |l2 )+', ''),
            '( un peu| uniquement| majoritaire| maternelle)+$', ''),
            '^[[:punct:][:space:]]+|[[:punct:][:space:]]+$', '')) as language_token
    from tokenised
),

-- Correspondance sur le token entier.
direct as (
    select
        c.family_id,
        c.language_token,
        m.language_label as labels,
        m.is_language
    from cleaned as c
    left join language_mapping as m
        on m.language_variant = c.language_token
    where c.language_token <> ''
      and length(c.language_token) > 1
),

-- Repêchage mot à mot. Certains parents séparent les langues par de simples
-- espaces ("turc kurd", "moldave français anglais"), que le découpage par
-- ponctuation laisse en un seul morceau. On ne découpe que les tokens restés
-- sans correspondance, pour ne pas casser les libellés en deux mots
-- ("arabe marocain", "créole haïtien") qui matchent déjà en entier.
fallback as (
    select distinct
        d.family_id,
        d.language_token,
        m.language_label as labels,
        m.is_language
    from direct as d,
    lateral flatten(input => split(d.language_token, ' ')) as w
    inner join language_mapping as m
        on m.language_variant = trim(w.value::string)
    where d.labels is null
),

resolved as (
    select family_id, language_token, labels, is_language
    from direct
    where labels is not null

    union all

    select family_id, language_token, labels, is_language
    from fallback

    union all

    -- Ni le token entier ni aucun de ses mots n'est connu du seed. Ces lignes
    -- sont conservées, avec language_label à null, pour rendre visible ce qui
    -- reste à mapper plutôt que de le perdre en silence.
    select d.family_id, d.language_token, null, null
    from direct as d
    left join fallback as f
        on f.family_id = d.family_id
       and f.language_token = d.language_token
    where d.labels is null
      and f.language_token is null
),

-- Une variante peut désigner plusieurs langues quand le parent les a écrites
-- sans séparateur ("français arabe"). Le seed les porte séparées par une
-- virgule, qu'on éclate ici.
mapped as (
    select
        r.family_id,
        r.language_token,
        r.is_language,
        nullif(trim(l.value::string), '') as language_label
    from resolved as r,
    lateral flatten(input => split(coalesce(r.labels, ''), ','), outer => true) as l
)

select
    fe.family_id,
    fe.cohort_name,
    fe.date_of_registration,
    fe.month_of_registration,
    fe.age_bucket_at_registration,
    fe.is_multilingual,
    mapped.language_token,
    mapped.language_label,
    mapped.language_label is null as is_unmapped,
    coalesce(mapped.is_language, false) as is_language,
    coalesce(mapped.language_label = 'Français', false) as is_french
from mapped
inner join family_enrollment as fe
    on fe.family_id = mapped.family_id
