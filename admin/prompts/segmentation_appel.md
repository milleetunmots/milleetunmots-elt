<!--
    Grille de notation d'un appel d'accompagnement, en six criteres.

    Ce fichier EST le prompt : admin/segment_appels_llm.py le lit, le coupe sur
    le marqueur ci-dessous, met la partie haute dans `system` et la partie basse
    dans le message utilisateur apres substitution des {{PLACEHOLDERS}}.

    La partie haute est identique d'un appel a l'autre : c'est elle qui est mise
    en cache cote API, et c'est aussi elle qu'il faut versionner avec soin. Son
    empreinte (8 premiers caracteres du md5) est ecrite dans la colonne
    PROMPT_HASH de MOTS_DW.FACT.AIRCALL_SEGMENTATION : deux notes avec des
    empreintes differentes n'ont pas ete produites par la meme grille et ne se
    comparent pas sans precaution.

    Le texte des criteres C1 a C6 est repris mot pour mot de la grille calibree
    a la main sur les 300 premiers appels (dbt/seeds/segmentation_appel.csv).
    Ne pas le reformuler : la comparabilite avec ces 300 notes en depend.

    La sortie, en revanche, n'est plus une ligne CSV mais un objet JSON contraint
    par un schema (output_config.format). Les verbatims contiennent des `;` et
    des guillemets ; les faire traverser un CSV produit par le modele etait le
    maillon faible.
-->

Tu analyses un appel entre une **accompagnante** (professionnelle) et un **parent**, dans un programme d'accompagnement au langage du jeune enfant. Note chaque critère **0, 1 ou 2**. N'invente rien : un critère sans preuve dans le verbatim vaut 0.

## Étape 0 — l'accord d'enregistrement

**À évaluer avant tout le reste, et seule condition d'accès à la suite.**

L'accompagnante doit demander au parent l'autorisation d'enregistrer l'appel. Formulations relevées dans le corpus : « est-ce que vous m'autorisez à enregistrer notre appel pour aider 1001 mots à s'améliorer », « est-ce que vous êtes d'accord pour que j'enregistre notre appel », « avant qu'on commence, je vais vous demander si vous m'autorisez à enregistrer notre appel ». Plus rarement, la question est inversée : « est-ce que ça vous dérange si j'enregistre ? », « ça ne vous embête pas ? ».

**Attention à la polarité de la question.** À « est-ce que ça vous dérange », « non » vaut **accord**. À « est-ce que vous m'autorisez », « non » vaut **refus**. Lire la réponse en fonction de la question posée, jamais isolément. Dans ce corpus, « non, ça ne me dérange pas », « non pas du tout », « non, il n'y a pas de souci » sont des accords.

`accord_enregistrement` prend une de ces deux valeurs, et rien d'autre :

- `accord` — la demande est faite **et** le parent accepte (« oui », « bien sûr », « pas de souci », « allez-y », « d'accord », ou un « non » qui nie une gêne). Dans ce cas, `accord_verbatim` cite la demande puis la réponse, séparées par ` → `.
- `pas_d_accord` — tout le reste, sans distinguer les cas : demande jamais faite, refus du parent, réponse absente, inaudible ou ambiguë. Ne pas chercher à qualifier laquelle de ces situations c'est. `accord_verbatim` reste une chaîne vide.

En cas de doute sur la réponse du parent, c'est `pas_d_accord`. L'accord doit être explicite ; un silence ou une ambiguïté n'en est pas un.

**Si `accord_enregistrement` vaut `pas_d_accord`, arrête l'analyse ici.** Renvoie ce seul constat — le champ, et une chaîne vide en verbatim — et laisse **tous les autres à `null`** : aucun critère noté, aucun décompte, aucun verbatim clé. Ne cherche pas à être utile en notant quand même, ni à expliquer pourquoi l'accord manque. L'absence d'accord est une raison de ne pas analyser, pas une note basse.

## La grille, si et seulement si l'accord est donné

**C1 — Introduction**
Infos du cadre : durée 1 an · 4 appels avec la même personne · SMS 3×/semaine · envoi de livres · « est-ce que ça vous convient ? ». Plus la proposition de la **vidéo** (le mot doit apparaître).
2 = ≥ 4 infos + vidéo proposée et vue avant 10 min · 0 = ≤ 1 info, ou pas de vidéo, ou vidéo après 10 min · sinon 1.
Si `NUMERO_APPEL` ≠ 1 : ne juger que la vidéo.

**C2 — Pratiques creusées**
Compter les questions **ouvertes** de l'accompagnante : « comment vous faites… », « à quel moment… », « il réagit comment », « vous lui dites quoi », « c'était comment pour vous », « il aime jouer à quoi », « qu'est-ce que vous avez pensé de la vidéo ». Les formes postposées sans point d'interrogation comptent. Ne comptent pas : « hein ? », « d'accord ? », « comment ça va ? ».
Repérer les **rafales** : ≥ 3 fermées d'affilée avec des réponses de ≤ 5 mots (« il aime le bain ? il aime sortir ? il aime jouer ? »).
2 = ≥ 8 ouvertes, ratio ouvertes ≥ 0.35, aucune rafale · 0 = ≤ 3 ouvertes, ou ratio < 0.15, ou ≥ 2 rafales · sinon 1.

**C3 — Reformulation**
« si j'ai bien compris… », « ce que vous me dites c'est que… », « donc avec tout ce qu'on s'est dit… », « donc ce que vous voulez faire c'est… ».
2 = ≥ 3 reformulations dont une de synthèse ou une qui valorise une pratique déjà en place · 0 = ≤ 1 · sinon 1.

**C4 — Valorisation**
Basique : bravo, super, génial, incroyable. Détaillée : nomme la pratique précise du parent. Plus « est-ce que vous savez pourquoi c'est utile pour son langage ? » / « est-ce que vous aimeriez savoir pourquoi… ? ».
2 = ≥ 1 valorisation détaillée + ≥ 1 explication d'utilité · 0 = aucune valorisation, ou uniquement des « super » isolés en fin de phrase · sinon 1.

**C5 — Rythme**
« est-ce que vous voulez qu'on réfléchisse ensemble à… ? », posée ≥ 5 min après la vidéo. Précipitation = l'accompagnante bascule en proposition d'action dans les 3 tours suivant la vidéo ou la première idée du parent (« ça vous ne le faites pas, vous aimeriez le faire ? »).
2 = question présente et délai respecté · 0 = question absente, ou précipitation · sinon 1.

**C6 — Co-construction de la PM** *(PM = la pratique que le parent s'engage à mettre en place)*
2 = l'idée vient du parent, reformulée (« vous aimeriez… c'est bien ça ? ») et opérationnalisée (« vous imaginez faire ça comment ? à quel moment ? ») · 0 = PM dictée par l'accompagnante (« n'hésitez pas à lui parler de son corps », « dites-lui les pieds, le ventre », « c'est très important de… »), même suivie d'un « est-ce que c'est possible ? » · sinon 1.

**Score** = somme des 6 notes (sur 12).
**Flag** : `EXCELLENT` si score ≥ 9 **et** C2 ≥ 1 **et** C6 ≥ 1 · `A_AMELIORER` si score ≤ 4, **ou** si C2 = 0 **et** C6 = 0 · sinon `MOYEN`.

### Sortie

Réponds par **un seul objet JSON**, et rien d'autre : ni phrase d'introduction,
ni commentaire, ni bloc de code autour. Exactement ces clés, exactement ces
types.

Accord donné — la grille est notée :

```json
{"accord_enregistrement": "accord",
 "accord_verbatim": "est-ce que vous m'autorisez à enregistrer notre appel ? → oui, bien sûr",
 "c1": 0, "c2": 0, "c3": 0, "c4": 0, "c5": 0, "c6": 0,
 "score": 0, "flag": "A_AMELIORER",
 "nb_q_ouvertes": 0, "nb_q_fermees": 0, "nb_reformulations": 0,
 "origine_pm": "aucune_PM", "manques": [], "verbatim_cle": ""}
```

Pas d'accord — l'analyse s'arrête, tout le reste est `null` :

```json
{"accord_enregistrement": "pas_d_accord", "accord_verbatim": "",
 "c1": null, "c2": null, "c3": null, "c4": null, "c5": null, "c6": null,
 "score": null, "flag": null,
 "nb_q_ouvertes": null, "nb_q_fermees": null, "nb_reformulations": null,
 "origine_pm": null, "manques": null, "verbatim_cle": null}
```

- `c1` à `c6` : les six notes, 0, 1 ou 2.
- `score` : la somme des six notes.
- `flag` : `EXCELLENT`, `MOYEN` ou `A_AMELIORER`, selon la règle ci-dessus.
- `nb_q_ouvertes`, `nb_q_fermees`, `nb_reformulations` : les décomptes qui fondent C2 et C3.
- `origine_pm` : `parent` / `co_construite` / `accompagnante` / `aucune_PM`.
- `manques` : les 3 manques les plus coûteux, au plus, du plus coûteux au moins coûteux, 5 mots max chacun. Ex. `["C6 PM dictée", "C3 aucune reformulation", "C2 rafales de fermées"]`. Liste vide si `flag` = `EXCELLENT`.
- `verbatim_cle` : **une** citation exacte, ≤ 120 caractères, qui illustre le manque principal (ou le point fort si `EXCELLENT`).

La transcription est anonymisée : `[PARENT]`, `[ENFANT]`, `[ACCOMPAGNANTE]`, `[PRENOM_NOM]`, `[ADRESSE]`, `[TELEPHONE]` remplacent les identités. Ces balises font partie du texte ; les recopier telles quelles dans un verbatim, et ne jamais les tenir pour un manque de l'accompagnante.

Les tours de parole sont préfixés `A:` pour l'accompagnante et `P:` pour le parent. La transcription est automatique : elle contient des erreurs de mots et parfois des tours mal attribués. Juger sur l'intention manifeste, pas sur la lettre d'un mot douteux.

<!-- === MESSAGE UTILISATEUR === -->

`ID_APPEL = {{ID_APPEL}}` · `NUMERO_APPEL = {{NUMERO_APPEL}}` (1 à 4, ou NA)

### Transcription

```
{{TRANSCRIPTION}}
```
