# Noter les appels d'accompagnement

La grille à six critères appliquée automatiquement aux transcriptions, dans le
prolongement des 300 notes posées à la main.

---

## 1. Ce que la grille mesure, et ce qu'elle ne mesure pas

### Étape 0 — l'accord d'enregistrement, condition d'accès à la grille

Avant tout critère, le modèle établit si l'accompagnante a demandé l'autorisation
d'enregistrer et si le parent l'a donnée. `accord_enregistrement` ne prend que
deux valeurs :

- `accord` — la demande est faite **et** le parent accepte. `accord_verbatim`
  cite alors la demande puis la réponse, pour qu'une vérification n'oblige pas à
  réécouter l'audio.
- `pas_d_accord` — tout le reste, sans distinction : demande jamais faite, refus,
  réponse absente ou ambiguë. Le modèle a pour consigne de ne pas qualifier
  laquelle de ces situations c'est, et `accord_verbatim` reste vide.

Deux valeurs et non quatre, parce que le besoin est binaire — a-t-on le droit
d'analyser cet appel — et que qualifier l'absence demandait un jugement de plus
sur l'élément le moins observable de la transcription. En cas de doute, c'est
`pas_d_accord` : l'accord doit être explicite.

**Sans accord, l'analyse s'arrête** : aucun critère n'est noté, `flag` vaut
`SANS_ACCORD` et les colonnes `c1` à `c6` sont **NULL — pas 0**. Un zéro se
mêlerait aux moyennes et ferait passer pour mauvaise une accompagnante qu'on n'a
pas le droit d'évaluer.

Ce jugement est confié au modèle, et pas à une regex, pour une raison mesurée :
**la polarité de la question s'inverse**. Le corpus contient 3 235 « est-ce que
vous m'autorisez à enregistrer notre appel » mais aussi 129 « est-ce que ça vous
dérange si j'enregistre ». Dans le second cas, « non » vaut accord — et « non,
ça ne me dérange pas » est la réponse la plus courante de cette famille. Un
classificateur par mots-clés la compte comme un refus.

La même mesure a montré la limite inverse : une regex couvrant les formulations
relevées trouvait une demande dans 53 % des appels réels, mais le modèle en
trouve dans des appels qu'elle manquait — « est-ce que vous êtes ok ou pas pour
qu'on enregistre notre conversation » n'était dans aucun motif. **53 % est un
plancher, pas le taux réel**, qui ne se connaîtra qu'après une passe.

### La grille, si l'accord est donné

Six critères, notés 0, 1 ou 2, sur le **déroulé de l'entretien** — pas sur son
résultat :

| | Critère | Ce qui le fait tomber à 0 |
|---|---|---|
| C1 | Introduction du cadre, vidéo proposée | vidéo jamais proposée, ou après 10 min |
| C2 | Pratiques creusées par des questions ouvertes | ≤ 3 ouvertes, ou des rafales de fermées |
| C3 | Reformulation | ≤ 1 reformulation |
| C4 | Valorisation nommée, utilité expliquée | aucune valorisation, ou des « super » isolés |
| C5 | Rythme, réflexion commune proposée à temps | question absente, ou précipitation |
| C6 | Co-construction de la petite mission | PM dictée par l'accompagnante |

Le score est la somme, sur 12.

### Le flag, et pourquoi il a changé

**Règle en vigueur (R1)** : `EXCELLENT` si score ≥ 9 **et** C2 ≥ 1 **et** C6 ≥ 1 ·
`A_AMELIORER` si score ≤ 4, **ou** si C2 = 0 **et** C6 = 0 · sinon `MOYEN`.

C2 et C6 restent le cœur de la posture — un appel à PM dictée ne peut pas être
`EXCELLENT` — mais il faut désormais que **les deux** échouent pour basculer en
`A_AMELIORER`, là où un seul suffisait.

La règle d'origine (R0 : `EXCELLENT` si score ≥ 10 et C2 = 2 et C6 = 2,
`A_AMELIORER` si score ≤ 5 **ou** C2 = 0 **ou** C6 = 0) a été abandonnée le
2026-10-06 sur une mesure : sur 399 notes, elle mettait 64 à 66 % des appels en
`A_AMELIORER` et 1 à 4 % en `EXCELLENT`. Un flag qui range deux tiers du corpus
dans la même case ne trie plus rien, et il disait surtout « C2 ou C6 vaut 0 » —
ce que les colonnes de critères disent déjà. Mesuré sur les mêmes notes :

| | R0 (abandonnée) | R1 (en vigueur) |
|---|---|---|
| `EXCELLENT` | 1 à 4 % | 10 à 21 % |
| `MOYEN` | 31 à 32 % | 45 à 53 % |
| `A_AMELIORER` | 64 à 66 % | 34 à 37 % |

**Changer de règle ne coûte pas une passe.** Le flag et le score sont recalculés
côté script à partir de C1…C6, jamais repris du modèle. Modifier `recalculer()`
puis lancer `--reflag` réétiquette toute la table — notes manuelles comprises,
pour que la colonne ne mélange pas deux règles — en une requête. La colonne
`regle_flag` dit sous quelle règle chaque ligne est étiquetée.

Ce que la grille ne dit pas : si la famille a progressé. Elle note ce que
l'accompagnante fait, pas l'effet obtenu. Pour l'effet, c'est
`.claude/skills/analyse-cohorte` et les taux d'appels OK.

Le texte des critères vit dans `admin/prompts/segmentation_appel.md` et nulle
part ailleurs. Il est repris mot pour mot de la grille calibrée à la main :
**ne pas le reformuler** sans conscience de rompre la comparabilité avec les 300
premières notes. Les versions se suivent par `prompt_hash`, empreinte de la
partie système du prompt :

| empreinte | ce qui change |
|---|---|
| `80090bc0` | première version ; les 99 notes Sonnet du 2026-10-06 en viennent. **Pas de vérification de l'accord d'enregistrement** |
| `59cb3b8c` | seuils de flag R1. Les six critères sont identiques |
| `08a2a3dc` | étape 0, l'accord d'enregistrement, et l'arrêt de l'analyse sans accord — avec quatre valeurs d'accord. Jamais passée en production |
| `96530a8b` | l'accord ramené à deux valeurs, `accord` ou `pas_d_accord`. Les six critères sont toujours identiques : les notes C1…C6 se comparent d'une version à l'autre |

**Les 99 notes en `80090bc0` ont été produites sans vérifier l'accord.** Elles
notent donc peut-être des appels qu'on n'avait pas le droit d'évaluer. Les
renoter sous `08a2a3dc` coûte sept minutes :

```bash
python admin/segment_appels_llm.py --random --seed 1001 --limit 100 \
    --model sonnet --workers 3 --force
```

`--force` est nécessaire : sans lui, l'anti-jointure les considère comme déjà
notés. La table étant en ajout seul, les nouvelles lignes supplantent les
anciennes dans `aircall_analysis` sans effacer la trace de la première passe.

---

## 2. La chaîne

```
fact.aircall_transcriptions_anonymised_by_call   (transcript anonymisé, 1 appel/ligne)
        │  filtres : duration > 400 s, is_anonymisation_complete,
        │             ≥ 150 caractères par minute d'appel
        │  puis, par famille et par session, le plus long appel seulement
        ▼
admin/segment_appels_llm.py  +  admin/prompts/segmentation_appel.md
        │  un `claude -p` par appel (abonnement), ou Batch API
        ▼
MOTS_DW.FACT.AIRCALL_SEGMENTATION      (ajout seul, 1 ligne par appel et par passe)
        │  qualify : la note la plus récente de chaque appel
        ▼
fact.aircall_analysis                  (colonnes segmentation_*)
```

Les 300 notes manuelles sont dans la même table, avec `origine_note = 'manuel'`.
`dbt/seeds/segmentation_appel.csv` reste dans le dépôt comme archive de cette
campagne, mais n'alimente plus rien : la reprise est faite, et la refaire ne
produirait que des doublons.

### Le périmètre

| | appels |
|---|---|
| appels réels (durée > 400 s) | 6 423 |
| dont anonymisation complète | 6 423 — la passe GLiNER est finie |
| moins ceux dont la transcription a échoué | −367 (voir §7) |
| moins les 300 déjà notés à la main et les 99 notés par Sonnet | −376 |
| moins les appels qui ne sont pas le plus long de leur session | −492 |
| **à noter** | **5 188** |

### Un seul appel par session, le plus long

Une session compte souvent plusieurs appels — rappels, lignes coupées, messages.
Le vrai entretien est le plus long ; les autres en sont des morceaux. Noter un
morceau produit une note sévère qui ne dit rien de l'accompagnante : sur la
première passe, l'appel 55284 a récolté 1/12 alors qu'il durait 12 minutes quand
le vrai échange de sa session en durait 18.

Et ces notes ne servaient à rien : `aircall_analysis` joint sur `max_duration`,
donc elle ne retient de toute façon que le plus long appel de la session. Sur
99 notes, 12 portaient sur un appel de rang 2 — du quota dépensé pour des lignes
que personne n'aurait lues. D'où la sélection, qui se fait **avant**
l'anti-jointure : sinon une session dont le plus long appel est déjà noté
ferait remonter le deuxième, et le problème reviendrait par la fenêtre.
`--tous-les-appels` rétablit l'ancien comportement si besoin.

En revanche, **aucun répondeur n'a été noté** : les 99 appels de la première
passe contiennent tous de la parole d'accompagnante, 8 segments au minimum. Le
filtre de densité (§7) suffit à les écarter.

Les 300 notes manuelles portent sur 234 appels rattachés à une ligne de
`aircall_analysis` ; les 66 autres concernent des appels qui n'étaient pas
l'appel représentatif de leur session. Elles ne sont pas perdues — lire la table
directement pour les voir.

---

## 3. Lancer

Deux moteurs, même grille, même table. Le défaut est l'abonnement.

| | `--engine claude-code` (défaut) | `--engine api` |
|---|---|---|
| ce qui paie | ton abonnement Claude | des jetons d'API facturés |
| mécanique | une invocation `claude -p` par appel | Batch API, lots de 2 000 |
| contexte | neuf à chaque appel | neuf à chaque appel |
| débit | ~1,3 appel/min par processus, quota permettant | tout le corpus en < 24 h |
| interruption | relancer, l'anti-jointure reprend | `--collect`, rien n'est repayé |

Le contexte neuf n'est pas un détail de mise en œuvre : c'est ce qui empêche la
note d'un appel d'influencer celle du suivant. Tes 300 notes manuelles ont été
posées comme ça, un appel à la fois.

### Sur abonnement

```bash
# le CLI, s'il n'est pas déjà là. Il partage les identifiants de l'extension
# VS Code : si tu y es connecté, il l'est aussi.
curl -fsSL https://claude.ai/install.sh | bash

# 1. un seul appel, commande et réponse imprimées en entier
python admin/segment_appels_llm.py --probe

# 2. ce que la passe représente en quota et en heures
python admin/segment_appels_llm.py --estimate

# 3. calibrer sur 30 appels déjà notés à la main
python admin/segment_appels_llm.py --only-notes --limit 30

# 4. par tranches, autant de fois que le quota l'impose
#    (--random --seed N pour un échantillon tiré au hasard et reproductible)
python admin/segment_appels_llm.py --limit 500 --workers 3

# 5. republier
dbt run -s aircall_analysis --profiles-dir dbt
```

**Commencer par `--probe`.** Les options passées à `claude` viennent de la
documentation, pas d'un essai sur cette machine : au moment de l'écriture, le
CLI n'y était pas installé. `--probe` lance un appel, imprime la commande exacte,
la réponse brute et le recalcul. Si une option n'existe pas sous ce nom, ça se
voit là, sur un appel, et `--claude-flag` permet de corriger sans toucher au
code :

```bash
python admin/segment_appels_llm.py --probe --claude-flag=--tools --claude-flag=''
```

`--json-schema` demande au CLI de contraindre la sortie au schéma au lieu de
laisser le script extraire le JSON du texte. À tester avec `--probe` : si le CLI
l'accepte, le garder ; sinon, l'extraction fonctionne très bien — elle gère les
blocs de code, les phrases autour et les accolades à l'intérieur des verbatims.

### Changer les seuils du flag

Sans renoter : modifier la règle dans `recalculer()` (une seule fonction, et la
seule copie de la règle), incrémenter `REGLE_FLAG`, puis

```bash
python admin/segment_appels_llm.py --reflag
dbt run -s aircall_analysis --profiles-dir dbt
```

Toute la table est réétiquetée en une requête, notes manuelles comprises — pour
que la colonne `flag` ne mélange jamais deux règles. `regle_flag` dit laquelle
s'applique à chaque ligne.

### Si le quota s'épuise

Le script s'arrête net, affiche le message du CLI (avec l'heure de remise à
zéro) et garde tout ce qui est déjà noté — l'écriture se fait par tranches de
20, pas en fin de passe. Relancer la même commande plus tard : les appels déjà
notés sont exclus par l'anti-jointure.

### Par l'API, si un jour le volume presse

```bash
pip install anthropic && export ANTHROPIC_API_KEY=...
python admin/segment_appels_llm.py --engine api --only-notes --limit 30
python admin/segment_appels_llm.py --engine api            # tout le corpus
python admin/segment_appels_llm.py --engine api --collect   # après interruption
```

### L'étape qui décide de tout

Le calibrage (`--only-notes`) renote des appels que tu as notés à la main et
imprime, critère par critère, la part de notes identiques, l'écart moyen et le
sens du biais (« le modèle note plus bas sur N appels »). Un accord médiocre sur
un seul critère se corrige en précisant ce critère dans le prompt ; un accord
médiocre partout veut dire que la grille suppose un contexte que la
transcription ne porte pas.

---

## 4. Ce que ça coûte

5 779 appels à noter, 95 Mo de transcription, 16 450 caractères et 19 minutes
par appel en moyenne. Soit **~32 M de jetons d'entrée**, 5 500 par appel grille
comprise.

**Sur abonnement**, ça ne se compte pas en argent mais en quota et en temps :
~36 h de temps de mur à deux processus en parallèle, hors attente, et des
fenêtres de quota (5 h, puis hebdomadaire) qui interrompront la passe plusieurs
fois. C'est praticable pour des centaines d'appels d'affilée, pas pour six mille
d'un coup. D'où `--limit` et `--sessions` : noter d'abord ce qui sert à une
question précise — une cohorte, une session, une accompagnante — plutôt que le
corpus entier par principe.

Trois choses à savoir avant de dimensionner une passe sur abonnement :

- **Chaque invocation paie le prompt système de Claude Code en plus de la
  grille.** Ce terme n'est pas documenté en jetons et il n'est pas forcément
  amorti d'une invocation à l'autre — chaque `claude -p` est un processus neuf.
  Il peut donc dominer les 6 000 jetons de la transcription. C'est ce que
  `--probe` mesure (`usage.input_tokens` dans l'enveloppe), et ce que la passe
  réaffiche ensuite par appel. `--claude-flag=--tools --claude-flag=''` enlève
  les définitions d'outils, qui en sont la plus grosse part.
- **Les limites sont par famille de modèle, pas pondérées par le prix.** Passer
  d'Opus à Sonnet ne divise pas la consommation par 2,5 : ça puise dans un autre
  réservoir, indépendant. Utile pour étaler, pas pour économiser.
- **Il n'y a pas de moyen documenté de lire le quota restant depuis un script.**
  L'enveloppe JSON donne les jetons de l'appel, pas ce qu'il reste. Le seul
  endroit qui l'affiche est `/usage` (ou `/cost`) dans un `claude` interactif.

**Par l'API**, le corpus entier passe en moins de 24 h pour ~100 à 240 $ selon
la profondeur de réflexion (`--effort`), remise Batch de 50 % comprise. Le
levier est l'effort, pas le modèle. Le coût réel par appel est imprimé en fin de
passe — c'est lui qui compte, pas l'estimation.

Dans les deux cas, le script imprime les jetons consommés. Sur abonnement il
affiche aussi l'équivalent en dollars que le CLI calcule : c'est une indication
de volume, rien n'est facturé à ce titre.

---

## 5. Les partis pris, à reconsidérer si le contexte change

**La sortie est du JSON, plus une ligne CSV.** La grille d'origine demandait au
modèle d'écrire `id;flag;score;…`. Les verbatims contiennent des `;` et des
guillemets ; un seul échappement manqué décale toute la ligne et produit un C3
là où il y avait un nombre de reformulations. Avec du JSON, cette classe
d'erreur disparaît : par l'API le schéma la rend impossible
(`output_config.format`), sur abonnement l'extraction vérifie l'équilibre des
accolades et refuse plutôt que de deviner. Les colonnes, elles, n'ont pas
changé.

**`score` et `flag` sont recalculés côté script.** Ce sont des fonctions
déterministes de C1…C6 : les demander au modèle ajoutait une source d'erreur
sans rien apporter. Le nombre d'appels où le modèle avait répondu autre chose
est imprimé en fin de passe — un taux qui grimpe signale un modèle qui lit mal
la grille, c'est un capteur gratuit. Et c'est ce qui a permis de changer les
seuils du flag le 2026-10-06 sans renoter un seul appel : un `--reflag`, une
requête, trente secondes. Si le modèle avait posé le flag lui-même, le même
changement aurait coûté une passe complète.

**Seuls les appels `is_anonymisation_complete` partent.** Un appel dont un seul
segment attend encore la passe NER sortirait du périmètre d'accès de la source
avec des prénoms de tiers en clair. Le filtre n'est pas négociable, et il n'y a
pas d'option pour le contourner.

**La table est en ajout seul, et chaque note porte l'empreinte de sa grille.**
`prompt_hash` donne les huit premiers caractères du md5 de la partie système
du prompt. Renoter un appel empile une ligne ; `aircall_analysis` ne garde que la
plus récente. Comparer deux grilles se fait en lisant la table et en filtrant
sur `prompt_hash`, pas en écrasant les notes précédentes.

---

## 6. Ce que la première passe a appris (99 appels, Sonnet, 2026-10-06)

Tirage aléatoire de 100 appels (`--random --seed 1001`), 99 notés, 6,9 minutes à
trois processus en parallèle. Mesures par appel : **11 140 jetons d'entrée** dont
21 % lus en cache, **1 490 de sortie** dont ~1 450 de réflexion. Soit 12,6 M de
jetons pour 1 000 appels, 73 M pour le corpus entier.

Comparé aux 300 notes manuelles :

| | manuel (300) | Sonnet (99) |
|---|---|---|
| flag `A_AMELIORER` (règle R1) | 34 % | 37 % |
| flag `MOYEN` | 45 % | 53 % |
| flag `EXCELLENT` | 21 % | 10 % |
| C1 — moyenne (part de 0) | 0,86 (46 %) | 0,93 (44 %) |
| C2 | 0,85 (34 %) | 0,90 (31 %) |
| C3 | 1,67 (9 %) | 1,37 (20 %) |
| C4 | 1,44 (16 %) | 1,30 (16 %) |
| C5 | 0,74 (53 %) | 0,47 (65 %) |
| C6 | 0,66 (54 %) | 0,45 (61 %) |
| PM `parent` | 23 % | **3 %** |
| PM `co_construite` | 20 % | **36 %** |
| PM `aucune_PM` | 34 % | 36 % |

Convergence sur les flags, C1, C2, C4 et l'absence de PM. Divergence sur la
sévérité de C3, C5 et C6, et **rupture sur l'origine de la PM** : là où la main
attribue l'idée au parent dans un quart des appels, le modèle ne le fait presque
jamais et classe `co_construite` à la place. C'est précisément le jugement qui
compte le plus pour le dispositif ; c'est donc là qu'il faut regarder avant de
se fier aux notes du modèle sur C6.

**Attention au raccourci** : ce ne sont pas les mêmes appels. Les 300 notes
manuelles ne sont pas un tirage aléatoire, donc une partie de chaque écart peut
venir de la composition des deux échantillons et non du correcteur. Le seul test
qui sépare les deux est `--only-notes` : renoter les mêmes appels et lire le
tableau d'accord.

**La note d'un appel isolé bouge d'un passage à l'autre.** Le même appel noté
deux fois a donné 8/12 puis 7/12, C1 à 1 puis 0, C5 à 2 puis 1 — flag et
origine de la PM inchangés. Il y a ~1 450 jetons de réflexion par appel et la
grille demande des jugements limites. Les agrégats sont solides, une note
individuelle est à ±1. Ne pas opposer un C1 à une accompagnante sur la foi d'un
seul passage.

**Environ 1 % des appels échouent** (1 sur 99 : réponse JSON tronquée, sur un
appel sans rien de particulier). Relancer la même commande les reprend.

---

## 7. Les pièges

**367 appels sont écartés parce que leur transcription a échoué, et c'est
mesuré, pas supposé.** Un appel transcrit normalement porte 865 caractères par
minute. 297 appels du corpus en portent moins de 50 : cinq segments et 176
caractères pour dix-neuf minutes de conversation, parfois un seul segment
`P: First.`. Les noter coûterait le prix d'un appel complet pour produire six
zéros qui ne disent rien de l'accompagnante — et, s'ils entraient dans les
moyennes, feraient passer pour mauvaises des personnes dont on n'a tout
simplement pas l'enregistrement. Le seuil est `--min-densite`, 150 caractères
par minute par défaut ; le nombre d'appels écartés est imprimé à chaque passe,
justement pour qu'on ne l'oublie pas. Le seuil absolu équivalent (2 000
caractères) écarte les mêmes appels à quatre près : la coupe est nette, il n'y a
pas de zone grise à arbitrer.

**`date_traite` n'est pas écrite par le script, c'est la valeur par défaut de la
colonne.** `write_pandas` pousse les horodatages pandas en nanosecondes dans un
`timestamp_ntz` : c'est ce qui a produit les dates en l'an 56765733 dans
`fact.aircall_transcriptions_ner`, où `dbt show` plante encore dès qu'on
sélectionne la colonne sans la caster. Ne pas « réparer » le script en
réintroduisant la colonne.

**`numero_appel` vaut `NULL` pour la session 0, pas `0`.** C'est la convention
des 300 notes manuelles (`NA` dans le seed), et le critère C1 en dépend : « si
`NUMERO_APPEL` ≠ 1, ne juger que la vidéo ». Une session 0 passée pour un
premier appel se verrait reprocher de ne pas introduire un cadre qu'elle n'a pas
à introduire.

**Le seuil de 400 secondes vient de `aircall_analysis.is_real_call`.** Les deux
doivent désigner le même sous-ensemble. En dessous, on note des répondeurs —
cf. le diagnostic d13 et le dossier des cohortes d'avril.

**La transcription est automatique.** Elle contient des mots faux et parfois des
tours de parole mal attribués. Le prompt le dit au modèle et lui demande de
juger sur l'intention manifeste. Une note isolée très basse mérite donc un coup
d'œil à la transcription avant d'être portée au débit d'une accompagnante : le
`verbatim_cle` est là pour rendre cette vérification immédiate.
