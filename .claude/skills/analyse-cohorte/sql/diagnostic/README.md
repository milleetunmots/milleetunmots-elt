# Requêtes de diagnostic

Les explorations menées en septembre 2026 sur la sous-performance des cohortes d'avril.
Chacune porte en tête la question posée, la méthode, et **le résultat obtenu** — la
plupart sont des hypothèses **écartées**, ce qui est l'essentiel de leur valeur : ne pas
les refaire.

Les fichiers marqués « chaque bloc est autonome » contiennent plusieurs requêtes séparées
par `;`, à lancer une par une.

| Fichier | Hypothèse | Verdict |
|---|---|---|
| `d01_denominateur_statuts.sql` | Les statuts vides sont-ils des appels oubliés ? | **Non** — ce sont des parcours arrêtés. Fonde la règle du dénominateur |
| `d02_calendrier.sql` | Vacances, ponts, période creuse | **Écartée**, confirmée par trois méthodes le 2026-10-02 (le test d'origine comparait des rangs de session différents) |
| `d03_mix_recrutement.sql` | Canal de recrutement | **RETENUE** — 10 pts entre caf et pmi, r = −0,87 avec l'écart |
| `d04_charge_travail.sql` | Surcharge, au niveau fenêtre et individuel | **Écartée** sur les deux niveaux |
| `d05_composition_familles.sql` | Âge, bilinguisme, délai, sources | Fait remonter le canal ; l'âge ne discrimine pas |
| `d06_import_en_lot.sql` | Import massif en un jour | Indice, pas preuve — 56 % sur un jour pour 04/06 |
| `d07_meme_source_dans_hors_cohorte.sql` | La source est-elle mauvaise, ou le lot ? | **Le lot** — 3 départements, −8 à −13 pts simultanés |
| `d08_dispersion_du_residu.sql` | Quelle taille de résidu est normale ? | **6,3 pts** — à lancer AVANT tout diagnostic |
| `d09_delai_creation_premier_appel.sql` | La famille a oublié l'accompagnement | **Écartée** — le délai ne prédit rien, avril a été appelée plus vite |
| `d10_joignabilite_aircall.sql` | Numéros morts | **Écartée** — décrochage identique, l'écart naît de la durée |
| `d11_heures_appel.sql` | Créneau horaire | **Écartée** — 5,5 pts entre créneaux, déficit dans tous |
| `d12_bilinguisme.sql` | Bilinguisme, maîtrise du français | **Écartée** — bilingues à +2 pts. Piège du NULL documenté |
| `d13_transcriptions_interlocuteur.sql` | Mauvais interlocuteur (numéros réattribués) | **ÉCARTÉE** — les appels courts sont des **répondeurs**. À durée égale, avril n'est jamais en dessous |
| `d14_numeros_vivants_sms.sql` | Numéros périmés | **ÉCARTÉE** — SMS délivrés (0,89 %) et familles qui répondent au taux normal (23,4 %) |
| `d15_trajectoire_et_groupes_rdv.sql` | Où est le trou, et combien de temps dure-t-il ? | **04/20 remonte en S1**. Déficit uniforme sur les 3 groupes RDV ; la masse est dans le groupe **non proposé** |
| `d16_rythme_et_effectif.sql` | Rythme d'appel, effectif de l'équipe dans la quinzaine | **Écartée** sur les deux niveaux |
| `d17_missions_qualite_echange.sql` | Les missions suivent-elles la durée d'échange ? | **Non** — la composition ne porte que 1,4 pt sur ~11 |
| `d18_session1_charniere.sql` | Pourquoi 2026/04/06 perd la moitié de sa cohorte | **EXPLIQUÉ** — règle métier : **2 appels KO consécutifs = désengagement automatique**. 04/06 est la seule cohorte à rater ses deux premières sessions |

## 2026/04/06 est expliquée — et c'était le bon dossier

Voir `d18`. L'attrition est **administrative** : l'application désengage automatiquement
une famille après **deux appels KO consécutifs**. `2026/04/06` est la seule cohorte de
2026 à avoir raté ses deux premières sessions (57,3 puis 46,3), donc la seule à envoyer
**26,5 % de ses familles** dans cette règle — la pire part de l'année. C'est de là que
vient la perte de la moitié de la cohorte, et de nulle part ailleurs.

Un KO isolé, lui, ne coûte rien : 98,2 % de couverture après (OK, KO). **Le coût d'une
mauvaise session dépend entièrement de la précédente.** C'est pourquoi `2026/04/20`, dont
la session 0 est bien plus basse (49,0 %), sauve sa taille : sa session 1 revient à 60,2 %.

Ce qui reste ouvert sur `2026/04/06`, c'est pourquoi ses familles CAF Oise sont devenues
difficiles à joindre en session 1 après une session 0 normale. Le déficit est bien attaché
à **ces familles** et non aux accompagnantes ni à la période (appariement à la semaine :
−11,3 pour elles contre **+2,7** pour les mêmes personnes sur leurs autres appels des
mêmes semaines, cf. `d18` bloc 8). Elles sont plus engagées que la moyenne — 22,6 %
envoient un SMS pendant la fenêtre contre 17-19 % — mais seules **46,7 % de celles qui
écrivent** obtiennent un appel OK, contre 62 à 83 % ailleurs. La lecture des SMS montre
des chaînes de reports qui ne convergent pas dans les 13 jours. Mécanisme décrit, cause
non établie.

C'est le dossier qui comptait, pas `2026/04/20` : la cohorte au taux le plus
spectaculaire n'était pas celle qui coûtait le plus. **Chercher l'attrition, pas le
taux.**

## Ce qui reste sur 2026/04/20

Mise à jour du **2026-10-02**, après l'arrivée des transcriptions Aircall.

**Le mécanisme est maintenant établi précisément, et les deux hypothèses qui restaient
en lice sont toutes les deux tombées.** Les appels d'avril aboutissent, sonnent
normalement (16 s), et tombent sur le **répondeur** plus souvent qu'ailleurs. Quand le
parent décroche, c'est le bon parent — zéro tiers sur 36 échanges lus. Les numéros sont
vivants : les SMS arrivent et les familles y répondent au taux des bonnes cohortes.
L'entonnoir fuit modestement à chaque étage et ça se cumule : 62,9 % de familles jointes
contre 68,2 %, 32,7 % d'entre elles sans entretien contre 23,9 %, 41,7 % d'OK après un
rappel convenu contre 47,0 %.

**La cause, elle, n'est toujours pas établie** — et la liste de ce qui a été écarté est
maintenant longue au point que c'est en soi le résultat. Six contraintes que toute
hypothèse nouvelle doit satisfaire simultanément :

1. Uniforme sur les trois groupes RDV, y compris les familles qui ont **choisi** leur
   créneau (69,4 % contre 90-92 % ailleurs), dont les créneaux ont bien été appelés.
2. La masse de l'écart (−10,7 des −19,6 pts contre 2026/05/04) est dans le groupe
   **non proposé**, donc sans lien avec le dispositif.
3. Diffus sur les accompagnantes (19 sur 22 sous leur projection, top 3 à 39 %).
4. **Réversible en 13 jours** : mêmes familles, mêmes numéros, S1 à 60,2 % contre 49,0 %
   en S0. Exclut toute propriété durable du fichier ou des familles.
5. N'affecte ni les SMS reçus, ni les SMS envoyés par les familles.
6. **Ne touche aucun autre appel passé les mêmes jours.** Vérifié par trois méthodes
   indépendantes (`d02`, blocs 2 et 3) : les cellules de 2026/03/23 S1 et 2026/02/23 S2,
   qui appelaient dans la même fenêtre, sont à +0,8 de la norme de leur session quand
   2026/04/20 S0 est à −9,0 ; l'indice de période hors cohortes d'avril pèse −0,0 point
   sur ces deux semaines ; et en appariant sur accompagnante × jour, l'écart reste de
   −4,9 points. Un autre **premier contact** dans la fenêtre suivante, 2026/05/04 S0,
   est parfaitement normal (−0,3) en plein pont du 1er et du 8 mai.

Et la mise en garde de `d08` reste la lecture la plus probable : −11,6 n'est qu'à
1,8 écart-type de la dispersion normale, et sur 42 cellules le minimum attendu par le
seul hasard est d'environ −13. **Il est possible qu'il n'y ait rien à expliquer.** La
seule piste encore ouverte est opérationnelle et porte au plus 4,5 points : la gestion
des créneaux pendant la montée en charge du dispositif RDV (voir `d15`, n = 8
transcriptions, hypothèse non tranchée).
