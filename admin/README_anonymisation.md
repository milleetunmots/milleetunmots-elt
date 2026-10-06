# Anonymiser les transcriptions Aircall

Mode opératoire et principes. À relire avant toute relance : plusieurs pièges de
ce pipeline ne se voient pas à l'exécution, ils se voient six mois plus tard sous
la forme d'un prénom resté en clair.

## 1. Le problème, et pourquoi il n'est pas celui qu'on croit

Aircall transcrit les appels et dépose le résultat dans
`MOTS_DL.MOTS_APP.AIRCALL_CALLS.RAW_TRANSCRIPTION_PAYLOAD` : un JSON par appel,
14 228 caractères en moyenne. À ce jour 22 174 appels transcrits, 301 Mo. Le
staging `stg_1001mots_app__aircall_call_transcriptions` l'aplatit en segments de
parole — environ 2 millions de lignes, 52 caractères chacune.

Le réflexe serait de passer le tout à un modèle de reconnaissance d'entités
(c'est ce que fait `admin/camille/anonymize.py` sur les `.docx` d'enquête). À
cette échelle c'est à la fois trop lent et, surtout, moins bon.

Moins bon, parce qu'un modèle *devine* qui est une personne, alors qu'ici **on le
sait**. Les transcriptions sont jointes à la base par `child_support_id` : on
connaît nommément le parent, la fratrie et l'accompagnante. 99,9 % des segments
se raccrochent à un dictionnaire d'identités. Un remplacement déterministe est
exact, vérifiable, et tourne en 10 secondes dans l'entrepôt.

D'où une architecture en deux passes : le dictionnaire pour ce que la base sait,
le modèle pour le reste seulement.

## 2. Les deux passes

### Passe 1 — dictionnaire (SQL, dans l'entrepôt)

Modèle `fact.aircall_transcriptions_dict`, macros dans
`dbt/macros/anonymise.sql`. Pour chaque famille, construit une alternation regex
à partir des noms connus et l'applique au texte :

| Tag | Source |
|---|---|
| `[PARENT]` | `parents.first_name` / `last_name`, parent 1 et 2 |
| `[ENFANT]` | `children.first_name` / `last_name`, toute la fratrie |
| `[ACCOMPAGNANTE]` | `admin_users.name` — celle de la famille, plus la liste globale du personnel (elles se citent entre elles) |
| `[TELEPHONE]`, `[EMAIL]` | regex, sans dictionnaire |

Couverture mesurée : environ 53 000 tags posés, dont 26 300 `[ENFANT]` et
24 900 `[ACCOMPAGNANTE]`. Les `[PARENT]` sont rares (1 900) et c'est normal : on
vouvoie le parent, on parle de l'enfant en permanence.

Le modèle marque ensuite chaque segment `is_candidat_ner` : reste-t-il un mot
capitalisé qui ne soit ni un nom connu, ni du vocabulaire courant ? Environ
73 000 segments, soit 3,7 %. C'est ce filtre qui rend la passe 2 abordable.

### Passe 2 — NER (GLiNER, en local)

Script `admin/anonymize_aircall_ner.py`. Traite uniquement les candidats, et
seulement leurs **textes distincts** : 73 000 segments ne portent que 68 000
textes différents, les formules de politesse et les messages de répondeur se
répétant d'un appel à l'autre.

Il détecte les tiers (`[PRENOM_NOM]`) et les adresses postales (`[ADRESSE]`) —
les seules PII que la base ne peut pas connaître, puisqu'elles n'y figurent pas.

Résultat écrit dans `MOTS_DW.FACT.AIRCALL_TRANSCRIPTIONS_NER`, clé = `md5(texte)`.
Clé sur le texte et non sur le segment : la table est vingt fois plus petite, et
une relance ne recalcule que les textes jamais vus. **L'anonymisation est donc
incrémentale sans machinerie** — c'est le point de conception le plus utile du
lot.

C'est licite parce que le NER tourne *après* le dictionnaire : deux segments de
même texte post-dictionnaire sont interchangeables, la part propre à la famille
ayant déjà été substituée.

### Sortie

| Modèle | Grain | Usage |
|---|---|---|
| `fact.aircall_transcriptions_anonymised` | segment | analyses fines : qui parle, à quel moment (cf. diagnostic d13) |
| `fact.aircall_transcriptions_anonymised_by_call` | appel | lecture d'un appel, recherche plein texte, BI |

État au 2026-10-06, après le premier passage complet : **100 % des 1 987 292
segments et des 20 067 appels anonymisés**, 95 143 tags posés.

| Tag | Nombre | Passe |
|---|---|---|
| `[ENFANT]` | 26 323 | dictionnaire |
| `[ACCOMPAGNANTE]` | 24 978 | dictionnaire |
| `[PARENT]` | 1 914 | dictionnaire |
| `[TELEPHONE]` | 1 152 | regex |
| `[PRENOM_NOM]` | 38 948 | NER |
| `[ADRESSE]` | 1 828 | NER |

La colonne `is_anonymisation_complete` dit si les deux passes ont été vues.
**Filtrer dessus dès que l'exhaustivité compte.** Attention au changement
d'échelle entre les deux grains : un appel compte 99 segments en moyenne, donc un
seul segment en attente suffit à marquer tout l'appel incomplet. Observé en cours
de premier passage : 96 % de segments complets correspondaient à 28 % d'appels
complets. Ce n'est pas un défaut de qualité, c'est une conjonction logique — et
les deux chiffres convergent vers 100 % une fois la passe NER terminée.

## 3. Relancer

```bash
# 1. passe dictionnaire + sortie (10 s pour 2 M de segments)
cd dbt && dbt run -s aircall_transcriptions_dict+ --profiles-dir .

# 2. vérifier que les macros n'ont pas régressé (14 cas, voir §5)
dbt test -s test_anonymise --profiles-dir .

# 3. passe NER — ne traite que les textes nouveaux
cd .. && python3 admin/anonymize_aircall_ner.py --device cpu --batch-size 64 --revue

# 4. refondre la sortie avec les nouveaux résultats NER
cd dbt && dbt run -s aircall_transcriptions_anonymised+ --profiles-dir .
```

Options utiles du script :

- `--limit N` : les N textes les plus fréquents seulement. Pour valider un
  changement de filtre sans engager la passe complète.
- `--dry-run` : n'écrit rien dans Snowflake. À combiner avec `--limit`.
- `--revue` : produit `revue_aircall_ner.html` (avant/après surligné) et
  `rapport_aircall_ner.csv` (journal de tous les remplacements). **Le CSV est
  l'outil de contrôle qualité : le trier par nombre de segments concernés met les
  erreurs les plus coûteuses en haut.**
- `--force` : retraite même les textes déjà en table. Nécessaire après toute
  modification des filtres, sinon l'anti-jointure les considère comme faits.

### Combien de temps

Passage complet mesuré (Apple M4, 10 cœurs, CPU) : **93 min pour 67 981 textes**,
soit 12,2 textes/s en moyenne.

Ne pas extrapoler depuis un `--limit` : les lots sont triés par longueur
croissante, donc le début de course n'est pas représentatif du tout. Débit
observé sur le run complet :

| Avancement | Débit |
|---|---|
| démarrage | 110 textes/s |
| 20 % | 55 textes/s |
| 60 % | 25 textes/s |
| 95 % | 12 textes/s |

Les 5 % de textes les plus longs portent 25 % des caractères — ils font 691
caractères en moyenne contre 146 pour le reste. Un étalonnage sur `--limit 1000`
annonçait 15 min, le run en a pris 93. L'estimation du script a été corrigée
depuis : elle raisonne en caractères restants et sur une fenêtre glissante, pas
en nombre de textes depuis le début.

Le tri par longueur reste une bonne optimisation — il évite de pader un segment
de 40 caractères à la taille d'un de 3 000 — mais il rend toute extrapolation
naïve systématiquement trop optimiste.

**CPU plutôt que MPS**, pour deux raisons. Il est plus rapide ici (89 contre 76
textes/s à l'étalonnage) : les segments sont trop courts pour amortir le
lancement des noyaux GPU. Et il est **reproductible** : au seuil de 0,30, MPS et
CPU ne trouvent pas exactement les mêmes entités, les différences d'arrondi
flottant faisant basculer les détections limites. Sur 1 000 textes l'écart était
de 175 contre 163 remplacements.

## 4. Les deux partis pris, à reconsidérer si le contexte change

**On sur-redacte plutôt que l'inverse.** Si un enfant s'appelle Rose, toutes les
« rose » du texte sautent. Aucun prénom n'est exclu du dictionnaire au motif
qu'il collisionne avec un mot courant : un faux positif coûte de la lisibilité,
un faux négatif coûte une réidentification.

**Les identifiants directs restent en clair.** `asset_url` (l'enregistrement
audio), `caller_id` et `phone_number` sont conservés tels quels — décision
explicite. Conséquence : **ces tables restent réidentifiables en un clic** via
l'audio, et n'ont donc pas vocation à sortir du périmètre d'accès de la source.
Si un partage externe est envisagé un jour, c'est la première chose à revoir.

Et dans tous les cas, ceci est de la pseudonymisation pour l'analyse, **pas un
effacement** : `raw_transcription_payload` reste intact dans `MOTS_DL`. Qui veut
l'effacement au sens du RGPD doit traiter la source elle-même.

## 5. Les pièges — lire avant de modifier les macros

Quatre bugs ont été trouvés en construisant ce pipeline. Aucun ne se voit sur un
échantillon : ils ne se manifestent que par quelques occurrences ratées dans des
millions de lignes. `dbt/tests/test_anonymise.sql` les verrouille en 14 cas ;
**le lancer après toute modification de `dbt/macros/anonymise.sql`.**

1. **`lower()` doit précéder `translate()`.** La liste d'accents est en
   minuscules, donc un prénom à initiale accentuée ressortait non déaccentué et
   son motif ne matchait que la graphie accentuée. 550 occurrences d'un même
   prénom à initiale accentuée restaient en clair.

2. **Le flag `'i'` de Snowflake ne replie pas la casse des accentués.** `É` n'y
   est pas reconnu comme équivalent de `é`. Les classes listent donc les deux
   casses explicitement : `[eéèêëEÉÈÊË]`.

3. **`\b` est ASCII-only, et c'est le plus vicieux.** Une lettre accentuée y
   compte comme non-mot, ce qui produit deux effets opposés. Un nom à initiale
   accentuée n'a aucune frontière détectable. Et surtout `\b` matche *à
   l'intérieur* des mots accentués : un prénom comme « A-rélie » se faisait
   couper à l'accent, « Auré[TAG] ». D'où
   des frontières explicites, avec les crochets exclus — sinon
   `ACCOMPAGNANTE` dans un tag déjà posé est lui-même un mot valide et se fait
   re-taguer (`[[[ACCOMPAGNANTE]]]`, 22 977 segments).

4. **La frontière de fin est consommée par le match**, donc deux noms adjacents
   perdaient le second (un prénom composé ne voyait taguer que sa première
   moitié). D'où les deux
   passes dans `scrub()`.

## 6. Les filtres du NER, et comment les régler

GLiNER sur du français parlé produit beaucoup de faux positifs : il étiquette
« person » sur des pronoms, des liens de parenté et tout mot capable d'ouvrir une
phrase — la reconnaissance vocale met une majuscule en tête de chaque segment.
Trois garde-fous, du plus général au plus spécifique :

**La stop-list** (`dbt_project.yml`, var `anonymise_stopwords`). Établie sur les
260 tokens capitalisés les plus fréquents du corpus : français courant, anglais
(le moteur bascule parfois de langue) et « 1001 mots » mal transcrit —
`milleamo`, `millermo`, `minermo`, `milamo`…

> Cette liste est lue **à la fois** par la macro SQL et par le script Python.
> C'est volontaire : chacun en gardait sa copie, elles ont dérivé, et GLiNER
> s'est mis à taguer « Allô » — 3 686 segments — comme un prénom. Ce qui aurait
> cassé le classificateur de répondeurs du diagnostic d13, lequel matche
> justement sur ces formules. Ne pas re-dupliquer.

**La liste blanche** (`load_gazetteer`). Tous les noms connus de la base, toutes
familles confondues : 43 000 noms. Une détection n'est retenue que si le mot est
un nom réellement porté par quelqu'un, ou si le score dépasse 0,75. Le
retournement est l'idée clé : en liste noire on joue au chat et à la souris, en
liste blanche on pose la bonne question. Et la liste n'a pas besoin de venir de
l'INSEE — un tiers cité dans un appel porte presque toujours un prénom déjà
enregistré chez une autre des 6 434 familles.

**Le vocabulaire courant déduit du corpus** (`load_common_words`). Un vrai nom
propre n'apparaît quasiment jamais en minuscules, un mot courant si. Mesuré :

| mot | minuscules | capitalisé | ratio |
|---|---|---|---|
| matin | 9 651 | 48 | 1,00 |
| fais | 16 052 | 37 | 1,00 |
| jamais | 8 489 | 83 | 0,99 |
| rien | 7 636 | 148 | 0,98 |
| vas | 2 655 | 139 | 0,95 |
| mohamed | 0 | 428 | 0,00 |
| thomas | 0 | 55 | 0,00 |
| rebecca | 0 | 19 | 0,00 |
| lily | 0 | 33 | 0,00 |

Le seuil à 0,75 sépare les deux populations sans recouvrement, **et la liste se
maintient d'elle-même** puisqu'elle est recalculée à chaque passage. C'est ce qui
évite d'allonger la stop-list à la main indéfiniment.

Effet cumulé mesuré sur 400 textes : 75 faux positifs ramenés à 28.

Un prénom qui est aussi un mot courant (« Rose ») tombe du mauvais côté et
échappe au NER — mais pas au dictionnaire, qui le connaît nommément pour sa
famille. La perte ne porte que sur les tiers homonymes.

## 7. Si la qualité se dégrade

Dans l'ordre :

1. Relancer `dbt test -s test_anonymise`. S'il échoue, le problème est dans les
   macros, pas dans le modèle.
2. Lire `rapport_aircall_ner.csv` trié par nombre de segments. Les faux positifs
   à fort volume sont en haut.
3. Un mot courant tagué à tort : vérifier d'abord s'il est passé sous le seuil de
   0,75 du ratio minuscule/capitalisé (il peut être trop rare en minuscules). Ne
   l'ajouter à la stop-list qu'en dernier recours — la stop-list ne passe pas à
   l'échelle, le ratio si.
4. Un vrai nom laissé en clair : vérifier qu'il est bien dans `admin_users`,
   `parents` ou `children`. Les bugs 1 à 3 du §5 étaient tous de cette forme.
5. Après tout changement de filtre, relancer avec `--force`, sinon l'anti-jointure
   saute les textes déjà traités.

## 8. État connexe à ne pas ignorer

`MOTS_DW.FACT.SUPER_TABLE_ANONYMISED2` porte un nom trompeur : seules 4 de ses
colonnes de texte libre ont traversé l'anonymiseur. `review_call0..3` (les notes
libres de l'accompagnante, qui nomment parents et enfants, 6 635 lignes),
`call1..4_previous_goals_follow_up` (7 377), `mid_term_reaction` (7 211) et
`stop_support_reason` (276) sont **remplies et en clair**. Elle est aussi périmée
de 8 colonnes par rapport à `SUPER_TABLE`.

Son script producteur `admin/anonymize_transcripts.py` ne tourne plus : il lit
`config["password"]` alors que `dbt/profiles.yml` est passé en authentification
par paire de clés. La connexion corrigée est dans
`admin/anonymize_aircall_ner.py:sf_connect`, à reprendre pour tout nouveau script.
