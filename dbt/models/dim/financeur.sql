-- Referentiel des financeurs suivis : UNE LIGNE ICI = UNE LIGNE DU TABLEAU du
-- dashboard « Suivi de projet v2 ». Liste, libelles et ordre arretes avec le
-- metier le 2026-11-10.
--
-- Pour ne plus afficher un financeur, retirer sa ligne ; pour en afficher un
-- nouveau, l'ajouter ici ET ajouter sa regle dans suivi_financeurs_enfant. Les
-- lignes de la feuille du bureau qu'aucun financeur ne reprend (ARS 10, ARS 63,
-- CD 69, CD 78, CAF 02, CAF 45) ne sont pas affichees : elles n'inscrivent plus
-- ou pas d'enfants, et leur engagement est nul ou vide. Le test
-- test_financeur_couverture_feuille echoue le jour ou l'une d'elles porte un
-- engagement non nul -- sans lui, cet engagement sortirait du total en silence.
--
--   ordre              l'ordre d'affichage : la Seine-Saint-Denis en tete, puis
--                      les territoires par numero de departement croissant.
--   financeur_court    le meme, pour l'axe d'un graphique.
--   regle_departement, regle_canal, regle_source
--                      la regle de rattachement telle que le metier la formule,
--                      un filtre du dashboard Mecenes et Partenaires par
--                      colonne : « Departement de la source », « Canal
--                      d'inscription », « Source precise ». Vide = filtre non
--                      utilise ; 'Tous' = tous les canaux du departement.
--                      C'est de la DOCUMENTATION : la regle executee est le
--                      case de suivi_financeurs_enfant. Changer l'une, c'est
--                      changer l'autre.
--   methode_de_calcul  les trois, en une phrase, pour un document ou un export.
--   codes_feuille      les lignes de la feuille du bureau (project_code de
--                      stg_projects__engagements) dont le financeur reprend
--                      l'engagement et la projection. Trois financeurs en
--                      regroupent deux : leurs engagements s'additionnent.

with financeurs (ordre, financeur_code, financeur, financeur_court, departement, regle_departement, regle_canal, regle_source, codes_feuille) as (
    select * from values
        ( 1, 'CD_93',       'CD de Seine-Saint-Denis (93)',                         'CD 93',         '93', '93',                                  'PMI',   null, 'CD_93'),
        ( 2, 'CAF_93',      'Caf de Seine-Saint-Denis (93)',                        'Caf 93',        '93', null,                                  null,    'CAF 93', 'CAF_93'),
        ( 3, 'CD_01',       'CD de l''Ain (01)',                                    'CD 01',         '01', '01',                                  'PMI',   null, 'CD_01'),
        ( 4, 'CAF_01',      'Caf de l''Ain (01)',                                   'Caf 01',        '01', null,                                  null,    'CAF Ain', 'CAF_01'),
        ( 5, 'DEP_10',      'Caf de l''Aube (10) et CD de l''Aube (10)',            'Caf + CD 10',   '10', '10',                                  'Tous',  null, 'CAF_10,CD_10'),
        ( 6, 'CD_27',       'CD de l''Eure (27)',                                   'CD 27',         '27', '27',                                  'PMI',   null, 'CD_27'),
        ( 7, 'CD_44',       'PMIs de Loire-Atlantique (44)',                        'PMI 44',        '44', '44',                                  'PMI',   null, 'CD_44'),
        ( 8, 'CD_45',       'CD du Loiret (45)',                                    'CD 45',         '45', '45',                                  'PMI',   null, 'CD_45'),
        ( 9, 'CAF_51',      'Caf de la Marne (51)',                                 'Caf 51',        '51', null,                                  null,    '[51] TS - CAF de la Marne', 'CAF_51'),
        (10, 'CE_REIMS',    'Cité Educative de la Croix-Rouge - Reims (51)',        'CE Reims',      '51', null,                                  null,    '[51] Cité éducative Reims Secteur SUD', 'CE_REIMS'),
        (11, 'CD_55',       'CD de la Meuse (55)',                                  'CD 55',         '55', '55',                                  'PMI',   null, 'CD_55'),
        (12, 'CD_57',       'CD de Moselle (57)',                                   'CD 57',         '57', '57',                                  'PMI',   null, 'CD_57'),
        (13, 'CAF_57',      'Caf de la Moselle (57)',                               'Caf 57',        '57', null,                                  null,    'CAF Moselle', 'CAF_57'),
        (14, 'DEP_60',      'Caf de l''Oise (60) et PMIs de l''Oise (60)',          'Caf + PMI 60',  '60', '60',                                  'Tous',  null, 'CAF_60,CD_60'),
        (15, 'DEP_63',      'Caf du Puy-de-Dôme (63) et PMIs du Puy-de-Dôme (63)',  'Caf + PMI 63',  '63', '63',                                  'Tous',  null, 'CAF_63,CD_63'),
        (16, 'MAIRIE_LYON', 'Ville de Lyon (69)',                                   'Ville de Lyon', '69', null,                                  null,    '[69] Ville de Lyon - Crèches municipales', 'MAIRIE_LYON'),
        (17, 'CE_PARIS_18', 'Cité Educative Paris 18e (75)',                        'CE Paris 18e',  '75', null,                                  null,    '[75] Cité éducative - Paris 18e · [75] Cité éducative Paris 18e - Crèche Huchard · [75] Cité éducative Paris 18e - Crèche Richomme · PMI Paris 18e - Binet · PMI Paris 18e - Cavé · PMI Paris 18e - Ney', 'CE_PARIS_18'),
        (18, 'CD_77',       'CD de Seine-et-Marne (77)',                            'CD 77',         '77', '77',                                  'PMI',   null, 'CD_77'),
        (19, 'CAF_77',      'CAF de Seine-et-Marne (77)',                           'Caf 77',        '77', null,                                  null,    'CAF Seine-et-Marne', 'CAF_77'),
        (20, 'CD_83',       'CD du Var (83)',                                       'CD 83',         '83', '83',                                  'PMI',   null, 'CD_83'),
        (21, 'CAF_83',      'Caf du Var (83)',                                      'Caf 83',        '83', null,                                  null,    'CAF Var', 'CAF_83'),
        (22, 'CAF_91',      'Caf de l''Essonne (91)',                               'Caf 91',        '91', null,                                  null,    'CAF Essonne', 'CAF_91'),
        (23, 'CD_91',       'CD de l''Essonne (91)',                                'CD 91',         '91', '91',                                  'PMI',   null, 'CD_91'),
        (24, 'CD_92',       'CD des Hauts-de-Seine (92)',                           'CD 92',         '92', '92, de la source ou de la famille',   'Tous',  null, 'CD_92'),
        (25, 'AUTRES',      'Autres petits partenaires',                            'Autres',        null, null,                                  null,    'Toute autre source, hors bouche-à-oreille (provisoire, à définir)', 'AUTRES')
)

select
    ordre,
    financeur_code,
    financeur,
    financeur_court,
    departement,
    regle_departement,
    regle_canal,
    regle_source,
    array_to_string(array_construct_compact(
        'Département de la source : ' || regle_departement,
        'Canal d''inscription : '    || regle_canal,
        'Source précise : '           || regle_source
    ), ' · ')                 as methode_de_calcul,
    split(codes_feuille, ',') as codes_feuille
from financeurs
