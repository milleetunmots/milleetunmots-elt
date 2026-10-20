# Les dashboards Metabase, versionnés

Le dashboard n'est pas l'objet de référence : **la spec l'est**. Un YAML dans
`metabase/dashboards/` décrit le dashboard, `admin/metabase/apply_dashboard.py`
le construit. Un dashboard retouché à la main dans l'interface sera écrasé au
prochain apply — c'est voulu, c'est ce qui rend la spec faisante foi.

Repris du même dispositif dans `happypal-elt`
(`scripts/metabase/apply_dashboard.py`), en nettement plus simple : ce projet n'a
ni couche de métriques Metabase ni manifest dbt à valider, donc **toutes les
cartes sont en SQL natif**.

## Utilisation

```bash
# valider la spec sans contacter Metabase
python admin/metabase/apply_dashboard.py metabase/dashboards/accompagnantes.yml --offline

# voir ce qui serait créé ou mis à jour — affiche un hash de plan
python admin/metabase/apply_dashboard.py metabase/dashboards/accompagnantes.yml

# appliquer le plan qu'on vient de lire
python admin/metabase/apply_dashboard.py metabase/dashboards/accompagnantes.yml --apply --confirm <hash>
```

Le hash sert à deux choses : on ne peut pas appliquer un plan qu'on n'a pas lu,
et si la spec ou Metabase ont bougé entre le plan et l'apply, le hash ne
correspond plus et l'apply refuse.

## Un dossier par dashboard

Chaque dashboard vit dans **son propre dossier** sous « Claude » :
`collection: Claude/Accompagnantes`. Le sous-dossier est créé s'il manque, et
les cartes y sont rangées avec le dashboard — on voit d'un coup d'œil ce qui lui
appartient, et on peut le partager ou l'archiver d'un bloc sans trier.

Conséquence à connaître : l'outil reconnaît une carte **par son nom dans le
dossier cible**. Déplacer un dashboard d'un dossier à l'autre sans déplacer ses
cartes ferait recréer les cartes à côté des anciennes, au lieu de les mettre à
jour. Déplacer l'ensemble d'abord, appliquer ensuite.

## Le garde-fou

Le script **refuse tout chemin dont la racine n'est pas « Claude »**. La clé
d'API a le droit d'écrire dans six autres collections, et rien côté Metabase
n'empêche d'écraser le travail d'une autre équipe : cette vérification est la
seule barrière, elle est donc dans le code et pas seulement dans une consigne.

## Authentification

`~/.config/metabase/1001mots.env`, chmod 600, hors du dépôt :

```
MB_API_KEY=...
MB_URL=https://metabase.1001mots.org
```

Clé créée dans Metabase (Administration → Authentification → Clés d'API),
utilisateur `dashboard-claude`, non-admin. Elle se révoque d'un clic sans effet
de bord sur les dashboards existants.

## Écrire une spec

Voir `accompagnantes.yml`, qui sert de modèle. Les points qui ne se devinent
pas :

**Les filtres catégoriels sont des *field filters*.** Metabase va chercher les
valeurs en direct dans la colonne — aucune liste en dur à maintenir, aucune
valeur qui périme. En contrepartie, deux contraintes :

- la table filtrée doit être **non aliasée** dans le SQL de la carte, sinon la
  clause générée ne résout pas ;
- le tag s'écrit `[[and {{slug}}]]`, **jamais `{{slug}}` nu**. Le tag rend une
  clause sans `and` devant, et `1=1` quand aucune valeur n'est choisie : écrit
  nu après un `where`, il produit `where ... 1=1` et la carte tombe en erreur de
  syntaxe. C'est l'erreur qui a coûté un aller-retour la première fois.

**Comparer « moi » à « la moyenne collective » demande deux lectures de la
table.** Le filtre s'injecte là où le tag est posé : s'il n'y a qu'une lecture,
il écrase aussi la moyenne, qui vaut alors toujours la valeur individuelle. La
spec `accompagnantes.yml` fait donc deux CTE — une filtrée, une non filtrée.

**Les onglets** se déclarent dans `tabs:`, et chaque carte dit dans quel onglet
elle vit avec `tab:`. La grille fait 24 colonnes de large. Un dashboard sans
onglets omet simplement `tabs:` et `tab:` — voir `fidelity.yml`.

**Les cartes `heading` et `text`** ne sont pas des questions : elles portent un
`text:` au lieu d'un `sql:`, vivent dans le dashboard seul et n'apparaissent
jamais dans la collection. C'est là qu'on met les mises en garde de lecture,
pour qu'elles voyagent avec le dashboard et pas seulement dans une conversation.

**Une carte retirée de la spec n'est pas supprimée** : elle sort du dashboard
mais reste dans le dossier, orpheline. L'archiver à la main
(`PUT /api/card/<id>` avec `archived: true`) — l'outil ne le fait pas, pour ne
jamais détruire sur la foi d'une spec.

**Tester le SQL avant d'appliquer.** Une carte qui échoue en production est une
carte de trop ; le script ne valide pas le SQL pour vous. Les requêtes se
rejouent sur Snowflake via `admin/sf_conn.py`, en remplaçant le tag par la
clause que Metabase générerait.

## Les dashboards existants

| Spec | Dossier | Dashboard | Pour qui |
|---|---|---|---|
| `accompagnantes.yml` | `Claude/Accompagnantes` | [119](https://metabase.1001mots.org/dashboard/119) | Les accompagnantes : le programme, et chacune face au collectif |
| `fidelity.yml` | `Claude/Suivi Fidelity` | [118](https://metabase.1001mots.org/dashboard/118) | Suivi des engagements du contrat Fidelity |

Les deux sont en spec : un renommage de colonne dans dbt se rattrape maintenant
en relançant l'apply, au lieu de casser une carte sans prévenir — ce qui était
déjà arrivé une fois au 118.
