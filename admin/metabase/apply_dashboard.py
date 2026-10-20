"""Construit un dashboard Metabase a partir de sa spec versionnee.

Le dashboard n'est pas l'objet de reference : la spec l'est. On lit un YAML de
metabase/dashboards/, on compare a ce qui existe dans Metabase, on affiche le
plan, et on applique seulement apres confirmation. Un dashboard modifie a la
main dans l'interface sera ecrase au prochain apply -- c'est voulu, c'est ce qui
rend la spec faisant foi.

Inspire du meme dispositif dans happypal-elt (scripts/metabase/apply_dashboard.py),
en nettement plus simple : ce projet n'a ni couche de metriques Metabase ni
manifest dbt a valider, donc toutes les cartes sont en SQL natif.

    python admin/metabase/apply_dashboard.py metabase/dashboards/accompagnantes.yml
    python admin/metabase/apply_dashboard.py <spec> --offline
    python admin/metabase/apply_dashboard.py <spec> --apply --confirm 3f9c1a20

Le plan affiche un hash ; --apply l'exige. Deux raisons : on ne peut pas
appliquer un plan qu'on n'a pas lu, et si la spec ou Metabase ont bouge entre le
plan et l'apply, le hash ne correspond plus et l'apply refuse.

Chaque dashboard vit dans SON PROPRE DOSSIER : `collection: Claude/<nom>`. Le
sous-dossier est cree s'il manque, et les cartes y sont rangees avec le
dashboard -- on voit d'un coup d'oeil ce qui lui appartient, et on peut le
partager ou l'archiver d'un bloc.

GARDE-FOU : la racine du chemin doit etre « Claude », sans quoi le script
refuse. La cle d'API a le droit d'ecrire ailleurs, et rien cote Metabase
n'empeche d'ecraser le travail d'une autre equipe -- cette verification est la
seule barriere, elle est donc dans le code et pas seulement dans une consigne.

L'authentification est lue dans ~/.config/metabase/1001mots.env (MB_API_KEY,
MB_URL), hors du depot.
"""

import argparse
import hashlib
import json
import os
import sys
import urllib.error
import urllib.request

import yaml

ENV_PATH = os.path.expanduser("~/.config/metabase/1001mots.env")
COLLECTION_AUTORISEE = "Claude"

# Cartes qui ne sont pas des questions : elles vivent dans le dashboard seul.
VIRTUELLES = ("heading", "text")


# --------------------------------------------------------------------- client


def charge_config():
    cfg = {}
    with open(ENV_PATH) as fichier:
        for ligne in fichier:
            ligne = ligne.strip()
            if ligne and "=" in ligne and not ligne.startswith("#"):
                cle, valeur = ligne.split("=", 1)
                cfg[cle.strip()] = valeur.strip()
    if not cfg.get("MB_API_KEY"):
        sys.exit("MB_API_KEY absent de %s" % ENV_PATH)
    return cfg


class Metabase:
    def __init__(self, cfg):
        self.url = cfg.get("MB_URL", "https://metabase.1001mots.org").rstrip("/")
        self.cle = cfg["MB_API_KEY"]

    def appel(self, methode, chemin, corps=None):
        requete = urllib.request.Request(self.url + chemin, method=methode)
        requete.add_header("x-api-key", self.cle)
        requete.add_header("Content-Type", "application/json")
        donnees = json.dumps(corps).encode() if corps is not None else None
        try:
            with urllib.request.urlopen(requete, donnees, timeout=120) as reponse:
                brut = reponse.read().decode()
                return json.loads(brut) if brut.strip() else None
        except urllib.error.HTTPError as erreur:
            detail = erreur.read().decode()[:500]
            raise SystemExit("Metabase %s %s -> HTTP %s\n%s" % (methode, chemin, erreur.code, detail))


# ------------------------------------------------------------------ resolution


def resout_collection(mb, chemin):
    """« Claude/Accompagnantes » -> l'id du sous-dossier, cree s'il manque.

    Chaque dashboard vit dans son propre dossier : ses cartes y sont rangees
    avec lui, on voit d'un coup d'oeil ce qui lui appartient, et on peut le
    partager ou l'archiver d'un bloc sans trier.

    Le garde-fou est ici et nulle part ailleurs : la racine du chemin doit etre
    « Claude ». Tout le reste du script suppose que la collection est legitime.
    """
    segments = [part.strip() for part in chemin.split("/") if part.strip()]
    if not segments or segments[0] != COLLECTION_AUTORISEE:
        sys.exit(
            "Collection « %s » refusee. Ce script n'ecrit que sous « %s/ ».\n"
            "La cle d'API a pourtant le droit d'ecrire ailleurs : c'est precisement\n"
            "pourquoi la limite est verifiee ici." % (chemin, COLLECTION_AUTORISEE)
        )

    toutes = mb.appel("GET", "/api/collection")
    racine = next((c for c in toutes if c.get("name") == COLLECTION_AUTORISEE
                   and (c.get("location") or "/") == "/"), None)
    if racine is None:
        sys.exit("Collection « %s » introuvable." % COLLECTION_AUTORISEE)
    if not racine.get("can_write"):
        sys.exit("Collection « %s » en lecture seule pour cette cle." % COLLECTION_AUTORISEE)

    courant, lieu = racine["id"], "/%s/" % racine["id"]
    for segment in segments[1:]:
        enfant = next((c for c in toutes if c.get("name") == segment
                       and (c.get("location") or "") == lieu), None)
        if enfant is None:
            enfant = mb.appel("POST", "/api/collection",
                              {"name": segment, "parent_id": courant})
            print("  dossier cree : %s" % segment)
        courant = enfant["id"]
        lieu = "%s%s/" % (lieu, courant)
    return courant


def resout_base(mb, nom):
    bases = mb.appel("GET", "/api/database")
    bases = bases["data"] if isinstance(bases, dict) and "data" in bases else bases
    for base in bases:
        if base["name"].lower() == nom.lower():
            return base["id"]
    sys.exit("Base « %s » introuvable. Disponibles : %s" % (nom, [b["name"] for b in bases]))


def resout_champ(mb, base_id, reference):
    """'FACT.SUPER_TABLE.SUPPORTER_NAME' -> l'id du champ Metabase."""
    schema, table, colonne = [part.upper() for part in reference.split(".")]
    metadonnees = mb.appel("GET", "/api/database/%d/metadata" % base_id)
    for tbl in metadonnees.get("tables", []):
        if tbl["name"].upper() == table and (tbl.get("schema") or "").upper() == schema:
            for champ in tbl.get("fields", []):
                if champ["name"].upper() == colonne:
                    return champ["id"]
            sys.exit("Colonne %s absente de %s.%s dans Metabase." % (colonne, schema, table))
    sys.exit("Table %s.%s absente du modele de donnees Metabase (synchronisation ?)." % (schema, table))


# ------------------------------------------------------------------ fabrication


def tags_modele(spec, carte, champs):
    """Les template-tags d'une carte : un par filtre qu'elle declare.

    Un filtre categoriel est un *field filter* : Metabase va chercher les
    valeurs en direct dans la colonne, il n'y a aucune liste en dur a maintenir.
    Il exige en revanche que la table filtree soit non aliasee dans le SQL de la
    carte, sinon la clause generee ne resout pas.
    """
    tags = {}
    for slug in carte.get("filters", []):
        filtre = next(f for f in spec["filters"] if f["slug"] == slug)
        tags[slug] = {
            "id": hashlib.md5(slug.encode()).hexdigest(),
            "name": slug,
            "display-name": filtre["display_name"],
            "type": "dimension",
            "dimension": ["field", champs[slug], None],
            "widget-type": filtre.get("type", "string/="),
        }
    return tags


def corps_carte(spec, carte, base_id, champs, collection_id):
    return {
        "name": carte["name"],
        "description": carte.get("description"),
        "display": carte.get("display", "table"),
        "visualization_settings": carte.get("visualization", {}) or {},
        "collection_id": collection_id,
        "dataset_query": {
            "type": "native",
            "native": {"query": carte["sql"], "template-tags": tags_modele(spec, carte, champs)},
            "database": base_id,
        },
    }


def cartes_existantes(mb, collection_id):
    elements = mb.appel("GET", "/api/collection/%d/items?models=card" % collection_id)
    elements = elements.get("data", elements) if isinstance(elements, dict) else elements
    return {item["name"]: item["id"] for item in elements}


def dashboards_existants(mb, collection_id):
    elements = mb.appel("GET", "/api/collection/%d/items?models=dashboard" % collection_id)
    elements = elements.get("data", elements) if isinstance(elements, dict) else elements
    return {item["name"]: item["id"] for item in elements}


# ------------------------------------------------------------------------ plan


def construit_plan(spec, mb, hors_ligne):
    plan = {"collection": spec["collection"], "dashboard": spec["name"],
            "tabs": list(spec.get("tabs", [])), "cartes": []}
    if hors_ligne:
        for carte in spec["cards"]:
            if carte.get("display") not in VIRTUELLES:
                plan["cartes"].append({"nom": carte["name"], "action": "?",
                                       "empreinte": empreinte_carte(carte)})
        return plan, None, None, None, {}

    collection_id = resout_collection(mb, spec["collection"])
    base_id = resout_base(mb, spec["database"])
    champs = {f["slug"]: resout_champ(mb, base_id, f["dimension"]) for f in spec.get("filters", [])}
    connues = cartes_existantes(mb, collection_id)
    for carte in spec["cards"]:
        if carte.get("display") in VIRTUELLES:
            plan["cartes"].append({"nom": carte["name"], "action": "texte",
                                   "empreinte": empreinte_carte(carte)})
            continue
        plan["cartes"].append({"nom": carte["name"],
                               "action": "mise a jour" if carte["name"] in connues else "creation",
                               "empreinte": empreinte_carte(carte)})
    plan["dashboard_action"] = ("mise a jour" if spec["name"] in dashboards_existants(mb, collection_id)
                                else "creation")
    return plan, collection_id, base_id, champs, connues


def empreinte_carte(carte):
    """Ce qui, dans une carte, doit invalider un plan deja affiche.

    Le hash du plan ne couvrait d'abord que les noms et les actions : modifier
    le SQL ou la description d'une carte laissait le hash inchange, donc un plan
    lu la veille restait applicable a une spec qui avait change entre-temps.
    C'est exactement ce que le hash est cense empecher.
    """
    corps = {cle: carte.get(cle) for cle in
             ("sql", "text", "description", "display", "visualization", "filters", "tab",
              "row", "col", "size_x", "size_y")}
    return hashlib.sha256(json.dumps(corps, sort_keys=True, ensure_ascii=False).encode()).hexdigest()[:10]


def empreinte(plan):
    return hashlib.sha256(json.dumps(plan, sort_keys=True, ensure_ascii=False).encode()).hexdigest()[:8]


def affiche_plan(plan, spec):
    print("Dashboard   %s  (%s)" % (plan["dashboard"], plan.get("dashboard_action", "?")))
    print("Collection  %s" % plan["collection"])
    print("Base        %s" % spec["database"])
    if plan["tabs"]:
        print("Onglets     %s" % ", ".join(plan["tabs"]))
    for filtre in spec.get("filters", []):
        print("Filtre      %s -> %s" % (filtre["display_name"], filtre["dimension"]))
    print()
    for carte in plan["cartes"]:
        if carte["action"] == "texte":
            continue
        print("  %-12s %s" % (carte["action"], carte["nom"]))
    print()


# ----------------------------------------------------------------------- apply


def applique(spec, mb, collection_id, base_id, champs, connues):
    ids = {}
    for carte in spec["cards"]:
        if carte.get("display") in VIRTUELLES:
            continue
        corps = corps_carte(spec, carte, base_id, champs, collection_id)
        if carte["name"] in connues:
            cid = connues[carte["name"]]
            actuelle = mb.appel("GET", "/api/card/%d" % cid)
            if actuelle.get("collection_id") != collection_id:
                sys.exit("La carte « %s » (id %d) vit hors de la collection cible." % (carte["name"], cid))
            actuelle.update(corps)
            mb.appel("PUT", "/api/card/%d" % cid, actuelle)
        else:
            cid = mb.appel("POST", "/api/card", corps)["id"]
        ids[carte["name"]] = cid
        print("  carte %-6s %s" % (cid, carte["name"]))

    existants = dashboards_existants(mb, collection_id)
    if spec["name"] in existants:
        did = existants[spec["name"]]
        actuel = mb.appel("GET", "/api/dashboard/%d" % did)
        if actuel.get("collection_id") != collection_id:
            sys.exit("Le dashboard « %s » vit hors de la collection cible." % spec["name"])
        mb.appel("PUT", "/api/dashboard/%d" % did,
                 {"name": spec["name"], "description": spec.get("description")})
    else:
        did = mb.appel("POST", "/api/dashboard", {"name": spec["name"],
                                                  "description": spec.get("description"),
                                                  "collection_id": collection_id})["id"]

    # Les onglets portent un id negatif a la creation ; Metabase les renumerote
    # et renvoie la correspondance dans sa reponse.
    onglets = [{"id": -(i + 1), "name": nom} for i, nom in enumerate(spec.get("tabs", []))]
    index_onglet = {nom: -(i + 1) for i, nom in enumerate(spec.get("tabs", []))}

    parametres = [{"id": hashlib.md5(f["slug"].encode()).hexdigest()[:8], "slug": f["slug"],
                   "name": f["display_name"], "type": f.get("type", "string/="),
                   "sectionId": "string"} for f in spec.get("filters", [])]

    dashcards, compteur = [], 0
    for carte in spec["cards"]:
        compteur += 1
        commun = {"id": -compteur, "row": carte["row"], "col": carte["col"],
                  "size_x": carte["size_x"], "size_y": carte["size_y"], "series": []}
        if spec.get("tabs"):
            commun["dashboard_tab_id"] = index_onglet[carte["tab"]]
        if carte.get("display") in VIRTUELLES:
            # Un titre ou un bloc de texte n'est pas une question : il vit dans
            # le dashboard seul, sous forme de « carte virtuelle ».
            dashcards.append({**commun, "card_id": None, "parameter_mappings": [],
                              "visualization_settings": {
                                  "virtual_card": {"name": None, "display": carte["display"],
                                                   "visualization_settings": {}, "dataset_query": {},
                                                   "archived": False},
                                  "text": carte["text"],
                                  "text.align_vertical": "top"}})
            continue
        mappings = []
        for slug in carte.get("filters", []):
            param = next(p for p in parametres if p["slug"] == slug)
            mappings.append({"parameter_id": param["id"], "card_id": ids[carte["name"]],
                             "target": ["dimension", ["template-tag", slug]]})
        dashcards.append({**commun, "card_id": ids[carte["name"]],
                          "parameter_mappings": mappings,
                          "visualization_settings": carte.get("card_settings", {}) or {}})

    charge = {"dashcards": dashcards, "parameters": parametres}
    if onglets:
        charge["tabs"] = onglets
    mb.appel("PUT", "/api/dashboard/%d" % did, charge)
    return did


# ------------------------------------------------------------------------ main


def main():
    arguments = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    arguments.add_argument("spec")
    arguments.add_argument("--apply", action="store_true")
    arguments.add_argument("--confirm")
    arguments.add_argument("--offline", action="store_true")
    options = arguments.parse_args()

    with open(options.spec) as fichier:
        spec = yaml.safe_load(fichier)
    for cle in ("name", "collection", "database", "cards"):
        if cle not in spec:
            sys.exit("Spec incomplete : « %s » manquant." % cle)
    racine_spec = spec["collection"].split("/")[0].strip()
    if racine_spec != COLLECTION_AUTORISEE:
        sys.exit("Collection « %s » refusee : ce script n'ecrit que sous « %s/ »."
                 % (spec["collection"], COLLECTION_AUTORISEE))

    mb = None if options.offline else Metabase(charge_config())
    plan, collection_id, base_id, champs, connues = construit_plan(spec, mb, options.offline)
    affiche_plan(plan, spec)

    if options.offline:
        print("Hors ligne : spec valide, rien compare ni applique.")
        return
    jeton = empreinte(plan)
    if not options.apply:
        print("Plan %s — pour appliquer :" % jeton)
        print("  python %s %s --apply --confirm %s" % (sys.argv[0], options.spec, jeton))
        return
    if options.confirm != jeton:
        sys.exit("Confirmation « %s » ne correspond pas au plan « %s ».\n"
                 "La spec ou Metabase ont bouge depuis : relancer le plan et relire."
                 % (options.confirm, jeton))

    did = applique(spec, mb, collection_id, base_id, champs, connues)
    print("\n%s/dashboard/%d" % (mb.url, did))


if __name__ == "__main__":
    main()
