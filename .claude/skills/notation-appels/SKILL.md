---
name: notation-appels
description: Note la qualité des appels d'accompagnement 1001 mots sur la grille à six critères — accord d'enregistrement, introduction du cadre, pratiques creusées, reformulation, valorisation, rythme et co-construction de la petite mission. À utiliser pour lancer ou relancer une passe de notation sur un corpus d'appels, pour modifier la grille ou les seuils de flag, pour reprendre une passe interrompue, ou dès qu'une question porte sur les notes des appels, sur le taux d'accord d'enregistrement, ou sur les colonnes segmentation_* de fact.aircall_analysis.
---

# Noter les appels d'accompagnement

Le job applique `admin/prompts/segmentation_appel.md` aux transcriptions
anonymisées et écrit une ligne par appel dans
`MOTS_DW.FACT.AIRCALL_SEGMENTATION`, que `fact.aircall_analysis` expose en
colonnes `segmentation_*`.

**La référence complète est `admin/README_segmentation.md`** : partis pris,
pièges, résultats mesurés, historique des versions de grille. Ce fichier-ci ne
dit que comment lancer le job et ce qu'il faut surveiller. Ne pas y recopier le
contenu du README — c'est en dupliquant une liste partagée que la stop-list du
NER a dérivé jusqu'à ce que GLiNER prenne « Allo » pour un prénom.

## Lancer une passe

Dans cet ordre. Chaque étape répond à une question que la suivante suppose
résolue.

```bash
# 0. le CLI, s'il manque. Il partage les identifiants de l'extension VS Code.
curl -fsSL https://claude.ai/install.sh | bash

# 1. un appel de contrôle : commande exacte, réponse brute, jetons consommés
python admin/segment_appels_llm.py --probe --model opus

# 2. ce que la passe représente en quota et en heures
python admin/segment_appels_llm.py --estimate --depuis 6

# 3. calibrer contre les 300 notes manuelles, sur les mêmes appels
python admin/segment_appels_llm.py --only-notes --limit 30 --model opus

# 4. la passe, par tranches si le quota l'impose
python admin/segment_appels_llm.py --depuis 6 --regrille --model opus --workers 4

# 5. republier : aircall_analysis est matérialisé en TABLE, pas en vue
dbt run -s aircall_analysis --profiles-dir dbt
```

Sélections utiles : `--depuis 6` ou `--depuis 2026-04-01`, `--sessions 1,2,3`,
`--random --seed N` pour un échantillon reproductible, `--limit N`.
`--regrille` renote ce que le modèle a noté sous une autre version de la grille
et laisse les notes manuelles intactes.

## Ce qui est déjà tranché, et sur quelle mesure

À ne pas rouvrir sans refaire la mesure qui l'a décidé.

| décision | mesure qui l'a tranchée |
|---|---|
| L'accord d'enregistrement est évalué en premier, et sans lui on ne note rien | 44 % des appels réels de la fenêtre n'ont pas de demande explicite |
| Deux valeurs d'accord, pas quatre | qualifier l'absence demandait un jugement de plus sur l'élément le moins observable |
| C'est le modèle qui juge l'accord, pas une regex | la polarité s'inverse : 3 235 « vous m'autorisez » contre 129 « ça vous dérange », où « non » vaut accord |
| Critères à `NULL` et non à `0` quand l'accord manque | un zéro se mêle aux moyennes et accuse une accompagnante qu'on n'a pas le droit d'évaluer |
| `score` et `flag` recalculés côté script | changer les seuils a coûté un `--reflag` de trente secondes au lieu d'une passe complète |
| Un appel par famille et par session, le plus long | 12 notes sur 99 portaient sur un fragment ; `aircall_analysis` joint sur `max_duration` et ne les affichait même pas |
| Transcription écartée sous 150 car/min | 367 appels tiennent en 5 segments pour 19 minutes de conversation |
| Opus plutôt que Sonnet | sur l'origine de la PM, Opus donne 21 % de « parent » contre 3 % pour Sonnet, et la main 23 % |

## Ce qu'il faut surveiller pendant une passe

**Le quota, et les faux positifs de sa détection.** Une passe de 1 429 appels a
été interrompue à 91 % par des motifs de détection trop larges qui matchaient
des champs de l'enveloppe JSON. Les motifs sont resserrés, et le texte complet
de tout échec est désormais dans `admin/logs/segmentation_echecs.log`. Avant de
conclure à un quota épuisé, le vérifier : `echo ok | claude -p "Reponds" --model
opus --tools "" --output-format json`.

**Une interruption ne coûte rien.** L'écriture se fait par tranches de dix et
l'anti-jointure exclut les appels déjà notés sous la grille courante : relancer
la même commande reprend exactement où ça s'est arrêté.

**Le débit.** Environ 13 appels/minute à quatre processus, pour 12 900 jetons
d'entrée et 1 300 de sortie par appel noté. Un appel sans accord se règle en
4 secondes, un appel noté en 25. Compter deux heures pour 1 500 appels.

## Lire les résultats

```sql
-- taux d'accord et profil des notes, par session
select coalesce(numero_appel::varchar, '0') as session,
       count(*)                                                     as notes,
       round(100.0 * count_if(accord_enregistrement = 'accord') / count(*)) as pct_accord,
       round(avg(score), 2)                                         as score,
       round(avg(c6), 2)                                            as c6_co_construction
from MOTS_DW.FACT.AIRCALL_SEGMENTATION
where prompt_hash = '<grille courante>'
group by 1 order by 1;
```

Toujours filtrer sur `prompt_hash` : la table est en ajout seul et porte
plusieurs versions de grille. `origine_note` sépare les notes du modèle
(`llm`) des 300 notes manuelles (`manuel`), `regle_flag` dit sous quelle règle
de flag la ligne est étiquetée.

**Deux lectures à ne pas confondre.** C1 note l'introduction du cadre et C5
dépend de la vidéo : ni l'un ni l'autre n'a de sens hors du premier appel, et ils
tirent mécaniquement le score des sessions tardives vers le bas. C6, lui, décroît
de 0,81 à 0,51 entre la session 0 et la session 3 sans qu'aucune règle de la
grille ne devienne plus sévère — c'est un résultat, pas un artefact.
