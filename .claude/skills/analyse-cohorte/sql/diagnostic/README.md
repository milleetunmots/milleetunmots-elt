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
| `d02_calendrier.sql` | Vacances, ponts, période creuse | **Écartée** — cellules jumelles sur la même fenêtre, résultats opposés |
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

## Ce qui reste

Après tout ça, le résidu des cohortes d'avril est de −11,6 points au call 0, dont le
mécanisme est établi (basculement des conversations longues vers les courtes) mais pas
la cause. Deux hypothèses subsistent et produisent la même signature : **numéros
réattribués** ou **familles moins engagées**. Voir la piste ouverte sur les
transcriptions dans `../../references/interpretation.md`.

Et la mise en garde de `d08` vaut pour tout ce dossier : −11,6 n'est qu'à 1,8 écart-type
de la dispersion normale.
