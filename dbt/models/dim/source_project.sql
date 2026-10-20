-- Rattachement de chaque source d'inscription au projet (= financeur) qui la
-- finance. C'est la table de correspondance a maintenir quand un partenariat
-- s'ouvre : tout le suivi des engagements en depend.
--
-- Regle generale : le canal et le departement de la source suffisent.
--   canal 'pmi' -> 'CD_<dept>'  (la PMI est un service du Conseil Departemental)
--   canal 'caf' -> 'CAF_<dept>'
-- Exceptions, etablies sur les commentaires saisis dans l'application :
--   - les Maisons des Solidarites de l'Aube sont saisies en 'local_partner'
--     alors qu'elles relevent du CD 10 ;
--   - l'AEMO / AED / DPPE de l'Essonne relevent du CD 91 ;
--   - les PMI du 18e arrondissement portent la mention « Financement par la
--     Cite Educative du 18e » : elles ne relevent donc pas du CD 75 ;
--   - « TS - CAF de la Marne » est saisi en 'local_partner' (code CAF_51) ;
--   - la creche Les Petits Hochets precise « inscription CAF 93 ».
-- Les sources explicitement de test ou de demonstration sortent du perimetre
-- (project_code = 'TEST') : elles portent des familles reelles mais qui ne
-- doivent etre comptees pour aucun financeur.
-- Le bouche-a-oreille et les reinscriptions directes ne sont pas finances :
-- code 'BAO', hors engagement, pour ne pas gonfler « Autres petits partenaires ».
-- Toute source dont le code calcule ne figure pas dans la feuille du bureau
-- bascule dans 'AUTRES', de sorte qu'aucune famille ne disparaisse du total.

with sources as (
    select * from {{ ref('stg_1001mots_app__sources') }}
),

-- Identite du projet, rattachee au code et non a l'annee : la feuille du bureau
-- ne porte qu'une annee, le libelle doit valoir pour toutes.
engagements as (
    select
        project_code,
        max_by(project_label, annee) as project_label
    from {{ ref('stg_projects__engagements') }}
    group by project_code
),

mapped as (
    select
        source_id,
        name as source_name,
        channel,
        nullif(department, '') as departement,
        utm,
        is_archived,
        case
            -- sources de test / demonstration
            when source_id in ('244', '253', '254')                       then 'TEST'
            -- financements explicites, hors regle canal x departement
            when source_id in ('350', '361')                              then 'CAF_51'
            when source_id = '164'                                        then 'CE_REIMS'
            when source_id = '162'                                        then 'MAIRIE_LYON'
            when source_id in ('73', '92', '158', '159', '160', '161', '339', '341')
                                                                          then 'CE_PARIS_18'
            when source_id = '78'                                         then 'CAF_93'
            when channel = 'local_partner' and department = '10' and name ilike '%MdS%'
                                                                          then 'CD_10'
            when channel = 'local_partner' and department = '91'          then 'CD_91'
            -- inscriptions sans financeur
            when channel in ('bao', 'other')                              then 'BAO'
            -- regle generale
            when channel = 'pmi' and nullif(department, '') is not null   then 'CD_'  || lpad(department, 2, '0')
            when channel = 'caf' and nullif(department, '') is not null   then 'CAF_' || lpad(department, 2, '0')
            else 'AUTRES'
        end as project_code_calcule
    from sources
)

select
    m.source_id,
    m.source_name,
    m.channel,
    m.departement,
    m.utm,
    m.is_archived,
    case
        when m.project_code_calcule in ('TEST', 'BAO', 'AUTRES') then m.project_code_calcule
        when e.project_code is not null then m.project_code_calcule
        else 'AUTRES'
    end as project_code,
    -- libelle lisible du financeur, pour que le detail se lise sans jointure.
    -- Les trois codes hors perimetre n'ont pas de ligne dans la feuille : ils
    -- portent leur propre libelle.
    case
        when m.project_code_calcule = 'TEST'   then 'Source de test'
        when m.project_code_calcule = 'BAO'    then 'Bouche-a-oreille, sans financeur'
        when e.project_label is not null       then e.project_label
        else 'Autres petits partenaires'
    end as project_label,
    -- vrai quand la source ne retombe sur aucune ligne de la feuille du bureau :
    -- a surveiller, c'est le signe d'un partenariat a declarer ou d'un departement
    -- nouveau
    case
        when m.project_code_calcule in ('TEST', 'BAO', 'AUTRES') then false
        when e.project_code is null then true
        else false
    end as is_hors_feuille,
    m.project_code_calcule
from mapped as m
left join engagements as e
    on e.project_code = m.project_code_calcule
