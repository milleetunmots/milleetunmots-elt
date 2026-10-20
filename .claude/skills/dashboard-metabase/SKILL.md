---
name: dashboard-metabase
description: Crée ou modifie un dashboard Metabase de 1001 mots à partir de sa spec YAML versionnée (metabase/dashboards/*.yml, appliquée par admin/metabase/apply_dashboard.py). À utiliser dès qu'une demande porte sur un dashboard, une question ou une carte Metabase — en créer un, en modifier un, ajouter ou retirer une carte, ajouter un filtre ou un onglet, réparer une carte cassée par un renommage de colonne dbt, ou comprendre pourquoi une carte ne renvoie rien.
---

# Dashboards Metabase

Le dashboard n'est pas l'objet de référence : **la spec l'est**. Ne jamais créer
ni modifier une carte par appels API directs — écrire ou modifier le YAML, puis
appliquer. Un dashboard retouché à la main dans l'interface sera écrasé au
prochain apply, et c'est voulu.

Référence complète du format et des conventions : `metabase/README.md`.

## Le déroulé, dans cet ordre

**1. Lire la spec existante** s'il y en a une (`metabase/dashboards/`). Les
en-têtes de spec portent les décisions prises et les raisons de les avoir
prises — notamment ce qu'il ne faut PAS ajouter au dashboard.

**2. Écrire le SQL et le tester sur Snowflake AVANT d'appliquer.** Le script ne
valide pas le SQL ; une carte qui échoue en production est une carte de trop.
Remplacer le tag de filtre par la clause que Metabase générerait :

```python
sql = carte["sql"].replace("[[and {{accompagnante}}]]",
    '''and "FACT"."SUPER_TABLE"."SUPPORTER_NAME" = 'exemple@mail.com' ''')
```

La connexion passe par `admin/sf_conn.py`, qui lit `dbt/profiles.yml`.

**3. Plan, puis apply avec le hash.**

```bash
python admin/metabase/apply_dashboard.py metabase/dashboards/<spec>.yml --offline  # valide la spec seule
python admin/metabase/apply_dashboard.py metabase/dashboards/<spec>.yml            # plan + hash
python admin/metabase/apply_dashboard.py metabase/dashboards/<spec>.yml --apply --confirm <hash>
```

**Lire le plan avant d'appliquer.** « creation » là où on attendait « mise a
jour » signale un nom de carte qui a changé, ou un dashboard qu'on s'apprête à
dupliquer.

**4. Vérifier chaque carte DEPUIS LE DASHBOARD**, pas isolément — une carte
peut répondre seule et échouer une fois le filtre branché :

```
POST /api/dashboard/<d>/dashcard/<dc>/card/<c>/query
```

avec, pour les cartes filtrées, un `parameters` portant une valeur de test.

## Les pièges, chacun a déjà coûté un aller-retour

**Le tag d'un field filter s'écrit `[[and {{slug}}]]`, jamais `{{slug}}` nu.**
Le tag rend une clause sans `and` devant, et `1=1` quand aucune valeur n'est
choisie : écrit nu après un `where`, il produit `where ... 1=1` et la carte
tombe en erreur de syntaxe.

**La table filtrée doit être non aliasée** dans le SQL de la carte, sinon la
clause générée ne résout pas.

**Comparer « moi » au collectif demande deux lectures de la table** — une qui
porte le tag, une qui ne le porte pas. Avec une seule, le filtre écrase aussi
la moyenne, qui vaut alors toujours la valeur individuelle.

**Un filtre ne s'applique qu'à l'endroit où le tag est posé.** Pour dépivoter
des sessions, scanner `super_table` UNE fois avec le filtre, puis dépivoter
depuis cette CTE — sinon seule la première branche de l'union est filtrée.

**Un taux ne s'additionne jamais, et Metabase l'additionne pourtant.** Si deux
lignes du résultat partagent la même valeur de dimension, Metabase **somme** les
séries — un graphique de pourcentages a affiché 154 % parce que deux cohortes
démarrent le 24 mars 2025. Agréger dans le SQL à la maille exacte de la
dimension tracée, en ratio de sommes : `sum(numérateur) / sum(dénominateur)`,
jamais une moyenne de taux. C'est pourquoi `analytics.kpi_fidelity_cohorte`
expose ses numérateurs bruts à côté de ses taux.

**Vérifier qu'aucune série déclarée ne manque du résultat.** `graph.metrics`
nomme les colonnes à tracer : renommer un alias SQL sans renommer la série fait
disparaître la courbe, sans erreur ni message. Le contrôle tient en deux lignes
— comparer `graph.metrics` aux colonnes rendues — et il a déjà attrapé le cas.

**Les vues `analytics.*` sont lisibles ET filtrables depuis le 2026-10-20**
(grant Snowflake + future grant, puis synchronisation du catalogue Metabase).
Toujours pointer les questions dessus plutôt que de recopier en SQL une logique
qui vit déjà dans un modèle dbt.
Attention, les deux conditions sont distinctes : un schéma fraîchement granté
est interrogeable en SQL natif mais **pas encore filtrable** tant que Metabase
n'a pas resynchronisé son catalogue — un field filter a besoin de l'identifiant
du champ. Si `resout_champ` échoue sur « table absente du modèle de données »,
c'est ça. `POST /api/database/3/sync_schema`.

## Les règles non négociables

**Un dossier par dashboard** : `collection: Claude/<nom>`. L'outil reconnaît
une carte par son nom *dans le dossier cible* — déplacer un dashboard sans ses
cartes crée des doublons. Déplacer l'ensemble d'abord, appliquer ensuite.

**N'écrire que sous `Claude/`.** La clé d'API a le droit d'écrire dans six
autres collections de l'instance, qui portent le travail d'autres équipes. Le
script refuse tout autre chemin ; ne pas contourner.

**Une carte retirée d'une spec n'est pas supprimée** : elle sort du dashboard
et reste orpheline dans le dossier. L'archiver à la main
(`PUT /api/card/<id>` avec `archived: true`).

## Les dénominateurs, pour tout dashboard d'appels

Reprendre les règles de la skill `analyse-cohorte`, elles ont été validées sur
données réelles :

1. **Dénominateur = les sessions dont le statut est renseigné.** Environ 12 %
   n'en ont pas, et ce sont presque exclusivement des parcours arrêtés.
2. **Une ligne par famille × session**, jamais par enfant — sauf reporting
   financeur, qui compte en enfants (voir `fact.suivi_fidelity_enfant`).
3. **Seules les sessions terminées** (`session_end <= current_date`).

Exception : le taux de petites missions a pour dénominateur les appels OK, qui
ont toujours un statut.

**Les cohortes exclues des analyses sont déjà hors de `fact.super_table`** —
elle les retire dans sa propre clause `where`, et sa colonne
`is_excluded_from_analytics` n'y vaut jamais vrai. Un filtre dessus est une
ceinture utile, pas le mécanisme. En revanche `dim.child`,
`fact.child_life_status` et `fact.suivi_engagements_enfant` **les contiennent** :
y lire sans filtrer surcompte. Dans `suivi_engagements_enfant` le drapeau
s'appelle `is_cohorte_exclue` — 914 enfants sur 2026.

**Exclure les enfants sans accompagnante affectée** (`supporter_name` vide) de
tout dénominateur qui n'est pas déjà filtré par un statut : ils sont ~600 sur
une rentrée, tous sans statut, et tirent le collectif vers le bas sans
correspondre à personne.

## Les dashboards existants

| Spec | Dossier | Dashboard |
|---|---|---|
| `accompagnantes.yml` | `Claude/Accompagnantes` | [119](https://metabase.1001mots.org/dashboard/119) |
| `fidelity.yml` | `Claude/Suivi Fidelity` | [118](https://metabase.1001mots.org/dashboard/118) |
| `suivi_projet.yml` | `Claude/Suivi de projet` | [120](https://metabase.1001mots.org/dashboard/120) |
