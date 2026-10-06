"""
Anonymisation des transcriptions Aircall -- passe 2 sur 2 : le NER.

La passe 1 (modele dbt fact.aircall_transcriptions_dict) a deja traite les
personnes qu'on connait nommement en base : le parent, la fratrie,
l'accompagnante, les numeros de telephone. Elle couvre 99,9 % des segments et
tourne en 10 secondes dans l'entrepot.

Reste ce que la base ne peut pas savoir : les tiers cites en passant (la
grand-mere, le medecin, une amie) et les adresses postales, qui ne figurent
nulle part dans les tables. C'est l'objet de ce script.

Deux choses le rendent tractable malgre les 103 Mo de corpus :

  * il ne traite que les segments marques is_candidat_ner par la passe 1 --
    ceux ou subsiste un mot capitalise hors vocabulaire courant, ou un mot de
    voie. 117 000 segments sur 1 987 000, soit 16 Mo au lieu de 103.
  * il dedoublonne par texte. Ces segments ne portent que 93 000 textes
    distincts : les formules de politesse et les messages de repondeur se
    repetent d'un appel a l'autre.

La table produite est donc clee sur le hash du texte, pas sur le segment. Elle
pese 93 000 lignes au lieu de 2 millions, et une relance ne recalcule que les
textes jamais vus -- l'anonymisation devient incrementale sans machinerie.

Usage :
    python admin/anonymize_aircall_ner.py --limit 500 --revue    # validation
    python admin/anonymize_aircall_ner.py                        # passe complete

Dependances :
    pip install gliner snowflake-connector-python pandas pyyaml tqdm
"""

import argparse
import csv
import hashlib
import html
import os
import re
import sys
import time
import unicodedata
from collections import deque

import pandas as pd
import yaml

from sf_conn import load_sf_config, sf_connect

BASE = os.path.dirname(os.path.abspath(__file__))

SOURCE_TABLE = "MOTS_DW.FACT.AIRCALL_TRANSCRIPTIONS_DICT"
TARGET_TABLE = "MOTS_DW.FACT.AIRCALL_TRANSCRIPTIONS_NER"

GLINER_MODEL = "urchade/gliner_multi_pii-v1"
GLINER_LABELS = ["person", "address", "phone number"]
# 0.30 comme dans admin/camille/anonymize.py : en dessous le modele part en
# vrille sur le francais parle, au-dessus il manque les prenoms rares. Le
# rattrapage se fait par les filtres ci-dessous, pas par le seuil.
GLINER_THRESHOLD = 0.30

TAG_PERSON = "[PRENOM_NOM]"
TAG_ADDR = "[ADRESSE]"
TAG_PHONE = "[TELEPHONE]"

# Les tags deja poses par la passe 1. GLiNER voit "[PARENT]" et y reconnait
# volontiers une personne : il faut l'empecher de re-taguer un tag.
TAG_RE = re.compile(r"\[[A-ZÀ-Þ_]+\]")


# ---------------------------------------------------------------------------
# Filtres de vraisemblance
#
# Repris de admin/camille/anonymize.py, ou ils ont ete calibres en relisant a
# la main les transcriptions des enquetes "Bilan appel 2" et "Livre bilingue".
# GLiNER sur du francais parle produit beaucoup de faux positifs : il etiquette
# "person" sur des pronoms, des liens de parente et des mots de trame. On ne
# redacte donc une detection que si elle ressemble vraiment a un nom propre.
# ---------------------------------------------------------------------------

COMMON_WORDS = {
    "je", "tu", "il", "elle", "on", "nous", "vous", "ils", "elles",
    "me", "te", "se", "lui", "leur", "leurs", "moi", "toi", "soi", "eux",
    "le", "la", "les", "un", "une", "des", "du", "de", "au", "aux",
    "mon", "ma", "mes", "ton", "ta", "tes", "son", "sa", "ses",
    "notre", "nos", "votre", "vos", "ce", "cet", "cette", "ces", "ca", "cela",
    "qui", "que", "quoi", "dont", "ou", "et", "a",
    # liens de parente et personnes generiques : non identifiants, on les garde
    # en clair pour que la transcription reste lisible
    "maman", "papa", "mamie", "papi", "papy", "meme", "pepe",
    "mere", "pere", "mari", "femme", "epoux", "epouse", "conjoint", "conjointe",
    "fille", "fils", "enfant", "enfants", "bebe", "petit", "petite",
    "frere", "soeur", "cousin", "cousine", "oncle", "tante", "parent", "parents",
    "grand", "grande", "grands", "grandmere", "grandpere", "belle", "beau",
    "monsieur", "madame", "mademoiselle", "mme", "mr", "mlle", "dame",
    "voisin", "voisine", "ami", "amie", "amis", "gens", "personne", "quelquun",
    "docteur", "medecin", "maitresse", "maitre", "nounou", "assistante",
    "puericultrice", "sage", "infirmiere", "orthophoniste", "creche",
}

def load_stopwords():
    """
    Le vocabulaire a ne jamais prendre pour un nom propre, lu dans
    dbt/dbt_project.yml (var anonymise_stopwords).

    Cette liste est partagee avec la macro SQL anonymise_stopwords(), qui s'en
    sert pour decider quels segments valent la peine d'un passage au NER. Les
    deux cotes en ont longtemps garde chacun sa copie ; elles ont derive, et
    GLiNER s'est mis a etiqueter "Allo" -- 3 686 segments -- comme un prenom.
    D'ou la source unique.
    """
    chemin = os.path.join(BASE, "..", "dbt", "dbt_project.yml")
    with open(chemin) as f:
        mots = yaml.safe_load(f)["vars"]["anonymise_stopwords"].split("|")
    # les comparaisons se font sur une forme sans accent et en minuscules
    return {strip_accents(m).lower() for m in mots if m}


NON_NAME_WORDS = {
    "nom", "noms", "prenom", "prenoms", "surnom", "pseudo",
    "accord", "celui", "ceci", "celle",
    "dormir", "livre", "livres", "langue", "langues",
    "ferme", "lequel", "pistes", "trame", "enquete", "notes",
}

NON_ADDR_WORDS = {
    "adresse", "adresses", "domicile", "maison", "logement", "appartement",
    "chez", "chez vous", "chez moi", "chez nous", "chez elle", "chez lui",
    "vos nouvelles adresses", "votre adresse", "mon adresse", "ici", "la",
}


def strip_accents(s):
    return "".join(
        c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn"
    )


def _is_namey_token(tok):
    """Un token qui ressemble a un nom propre : majuscule initiale, >= 2 lettres."""
    if len(tok) < 2:
        return False
    if not re.fullmatch(r"[A-Za-zÀ-ÿ'’\-]+", tok):
        return False
    base = strip_accents(tok).lower()
    if base in COMMON_WORDS or base in NON_NAME_WORDS:
        return False
    return tok[0].isupper()


def is_real_person(span_text):
    toks = [t for t in re.split(r"[\s\-']+", span_text.strip()) if t]
    return any(_is_namey_token(t) for t in toks) if toks else False


def is_real_address(span_text):
    """Ecarte les tournures generiques ("chez vous") sans information localisante."""
    norm = strip_accents(span_text.strip()).lower()
    if norm in NON_ADDR_WORDS:
        return False
    has_digit = any(c.isdigit() for c in span_text)
    has_way = bool(re.search(
        r"\b(rue|avenue|av|boulevard|bd|impasse|all[ée]e|place|chemin|route|"
        r"quai|cours|square|villa|r[ée]sidence|lotissement|cit[ée])\b", norm))
    # Un simple mot capitalise ne fait pas une adresse : c'est ainsi que
    # "Lundi" se retrouvait tague [ADRESSE]. Il faut un numero ou un mot de
    # voie. Les communes, elles, restent en clair -- convention deja retenue
    # dans SUPER_TABLE_ANONYMISED2, qui conserve parent1_city_name.
    return has_digit or has_way


def is_real_phone(span_text):
    return sum(c.isdigit() for c in span_text) >= 4


# La stop-list partagee vient completer les deux ensembles ci-dessus.
NON_NAME_WORDS |= load_stopwords()

# Liste blanche de vrais noms, remplie au demarrage par load_gazetteer().
GAZETTEER = set()
# Au-dela de ce score, on fait confiance a GLiNER meme hors liste blanche : un
# nom rare ou etranger absent de la base doit pouvoir passer.
SCORE_HORS_GAZETTEER = 0.75


def load_gazetteer(conn):
    """
    Tous les noms connus de la base, toutes familles confondues.

    C'est le retournement qui rend GLiNER exploitable ici. En liste noire, on
    joue au chat et a la souris : chaque mot francais capable d'ouvrir une
    phrase finit par etre pris pour un prenom ("Vas-y", "Aucun", "Jamais",
    "Absolument"...). En liste blanche, on demande l'inverse : ce mot est-il un
    nom reellement porte par quelqu'un ?

    Et la liste n'a pas besoin de venir de l'INSEE : un tiers cite dans un appel
    -- la grand-mere, une cousine, le medecin -- porte presque toujours un
    prenom deja enregistre chez une autre des 6 434 familles. 43 500 noms
    distincts, dont 28 seulement collisionnent avec la stop-list.
    """
    sql = """
        with noms as (
            select first_name as p from MOTS_DW.STAGING.STG_1001MOTS_APP__PARENTS
            union select last_name  from MOTS_DW.STAGING.STG_1001MOTS_APP__PARENTS
            union select first_name from MOTS_DW.STAGING.STG_1001MOTS_APP__CHILDREN
            union select last_name  from MOTS_DW.STAGING.STG_1001MOTS_APP__CHILDREN
            union select name       from MOTS_DW.STAGING.STG_1001MOTS_APP__ADMIN_USERS
        )
        select distinct trim(p)
        from noms
        where length(trim(p)) >= 3 and regexp_like(trim(p), '[A-Za-zÀ-ÿ]+')
    """
    cur = conn.cursor()
    cur.execute(sql)
    out = set()
    for (nom,) in cur.fetchall():
        # un nom composé entre aussi en morceaux : "Marie-Claire" doit valider
        # une detection sur "Claire" seul
        for tok in re.split(r"[\s\-']+", nom):
            tok = strip_accents(tok).lower()
            if len(tok) >= 3 and tok not in NON_NAME_WORDS:
                out.add(tok)
    return out


def in_gazetteer(span_text):
    toks = [strip_accents(t).lower() for t in re.split(r"[\s\-']+", span_text.strip())]
    return any(t in GAZETTEER for t in toks if t)


# Mots que le corpus lui-meme designe comme courants : voir load_common_words().
COMMON_CORPUS_WORDS = set()
RATIO_MINUSCULE = 0.75
MIN_OCCURRENCES = 20


def load_common_words(conn):
    """
    Le vocabulaire courant, deduit du corpus au lieu d'etre liste a la main.

    Allonger une stop-list mot par mot est sans fin : tout mot francais capable
    d'ouvrir une phrase finira par etre pris pour un prenom, parce que la
    reconnaissance vocale met une majuscule en tete de chaque segment. Il faut
    un critere, pas une liste.

    Celui-ci : un vrai nom propre n'apparait quasiment jamais en minuscules,
    un mot courant si. Mesure sur le corpus --

        matin   9 651 min. /    48 cap.  ratio 1,00     mohamed   0 / 428  0,00
        fais   16 052 /    37         1,00              thomas    0 /  55  0,00
        jamais  8 489 /    83         0,99              rebecca   0 /  19  0,00
        rien    7 636 /   148         0,98              lily      0 /  33  0,00
        vas     2 655 /   139         0,95              luis      0 /  43  0,00

    Le seuil a 0,75 separe les deux populations sans recouvrement. Et la liste
    se maintient d'elle-meme : elle est recalculee a chaque passage.

    Un prenom qui est aussi un mot courant ("Rose") tombe du mauvais cote et
    echappe au NER -- mais pas au dictionnaire de la passe 1, qui le connait
    nommement pour sa famille. La perte ne porte que sur les tiers homonymes.
    """
    sql = f"""
        with toks as (
            select f.value::string as tok
            from {SOURCE_TABLE},
                lateral flatten(input => split(
                    regexp_replace(text_dict, '[^A-Za-zÀ-ÿ ]', ' '), ' ')) f
            where length(f.value::string) >= 3
        ),
        agg as (
            select lower(translate(tok, 'àâäéèêëîïôöùûüç', 'aaaeeeeiioouuuc')) as mot,
                   count_if(tok = lower(tok))  as n_min,
                   count_if(tok != lower(tok)) as n_cap
            from toks group by 1
        )
        select mot from agg
        where n_min >= {MIN_OCCURRENCES}
          and n_min / (n_min + n_cap) >= {RATIO_MINUSCULE}
    """
    cur = conn.cursor()
    cur.execute(sql)
    return {mot for (mot,) in cur.fetchall()}

def pick_device(prefere="auto"):
    """
    Choisit le peripherique d'inference.

    Sur Apple Silicon, MPS fait tourner le transformeur sur le GPU integre :
    c'est le gain le plus important disponible ici, bien avant le
    multiprocessing. GLiNER tombe parfois sur une operation non implementee en
    MPS, d'ou le repli automatique sur CPU dans run_inference().
    """
    import torch
    if prefere == "cpu":
        return "cpu"
    if prefere in ("auto", "mps") and torch.backends.mps.is_available():
        return "mps"
    if prefere in ("auto", "cuda") and torch.cuda.is_available():
        return "cuda"
    return "cpu"


FILTERS = {
    "person": (is_real_person, TAG_PERSON),
    "address": (is_real_address, TAG_ADDR),
    "phone number": (is_real_phone, TAG_PHONE),
}


# ---------------------------------------------------------------------------
# Application des detections
# ---------------------------------------------------------------------------

def tag_spans(text):
    """Positions des tags deja poses par la passe 1, a ne pas re-taguer."""
    return [(m.start(), m.end()) for m in TAG_RE.finditer(text)]


def anonymize(text, entities):
    """
    Applique les detections retenues, de droite a gauche pour que les positions
    restent valides. Renvoie (texte, remplacements).
    """
    protege = tag_spans(text)
    retenues = []
    for ent in entities:
        label = ent.get("label")
        if label not in FILTERS:
            continue
        predicat, tag = FILTERS[label]
        span = text[ent["start"]:ent["end"]]
        if not predicat(span):
            continue
        # Pour une personne, le filtre structurel ne suffit pas : il laisse
        # passer tout mot capitalise. Deux garde-fous s'ajoutent.
        if label == "person" and GAZETTEER:
            # les tokens d'une ou deux lettres ne peuvent pas porter la
            # detection : sans ce seuil, "Vas-y" passait sur son "y", "vas"
            # etant pourtant reconnu comme mot courant
            toks = [strip_accents(t).lower()
                    for t in re.split(r"[\s\-']+", span.strip()) if len(t) >= 3]
            if not toks:
                continue
            # (a) une detection faite uniquement de mots courants du corpus
            # n'est pas un nom, quel que soit l'aplomb du modele
            signifiants = [t for t in toks if t not in COMMON_CORPUS_WORDS]
            if not signifiants:
                continue
            # (b) sinon il faut un nom reellement porte en base, ou un score
            # eleve pour les noms rares que la base ne connait pas
            connu = any(t in GAZETTEER for t in signifiants)
            if not connu and ent.get("score", 1.0) < SCORE_HORS_GAZETTEER:
                continue
        # jamais a l'interieur d'un tag deja pose
        if any(ent["start"] < fin and ent["end"] > debut for debut, fin in protege):
            continue
        retenues.append((ent["start"], ent["end"], tag, span))

    # resolution des chevauchements : on garde la detection la plus longue
    retenues.sort(key=lambda r: (-(r[1] - r[0]), r[0]))
    gardees, occupes = [], []
    for debut, fin, tag, span in retenues:
        if any(debut < f and fin > d for d, f in occupes):
            continue
        gardees.append((debut, fin, tag, span))
        occupes.append((debut, fin))

    gardees.sort(key=lambda r: r[0])
    out, last = [], 0
    for debut, fin, tag, _ in gardees:
        out.append(text[last:debut])
        out.append(tag)
        last = fin
    out.append(text[last:])
    return "".join(out), gardees


# ---------------------------------------------------------------------------
# Snowflake  (la connexion elle-meme vit dans admin/sf_conn.py)
# ---------------------------------------------------------------------------


def fetch_todo(conn, limit=None, force=False):
    """
    Les textes candidats encore jamais passes au NER.

    L'anti-jointure sur la table cible est ce qui rend les relances bon marche :
    apres un premier passage complet, un run quotidien ne voit que les textes
    des nouveaux appels.
    """
    deja = ""
    if not force:
        conn.cursor().execute(
            f"""create table if not exists {TARGET_TABLE} (
                    text_hash   varchar,
                    text_dict   varchar,
                    text_ner    varchar,
                    nb_tags     integer,
                    date_traite timestamp_ntz
                )"""
        )
        deja = f"and md5(text_dict) not in (select text_hash from {TARGET_TABLE})"

    sql = f"""
        select md5(text_dict) as text_hash, any_value(text_dict) as text_dict,
               count(*) as nb_segments
        from {SOURCE_TABLE}
        where is_candidat_ner {deja}
        group by 1
        order by nb_segments desc
        {f'limit {int(limit)}' if limit else ''}
    """
    cur = conn.cursor()
    cur.execute(sql)
    rows = cur.fetchall()
    return pd.DataFrame(rows, columns=["text_hash", "text_dict", "nb_segments"])


def write_results(conn, df):
    from snowflake.connector.pandas_tools import write_pandas
    db, schema, table = TARGET_TABLE.split(".")
    out = df[["text_hash", "text_dict", "text_ner", "nb_tags"]].copy()
    out["date_traite"] = pd.Timestamp.utcnow().tz_localize(None)
    out.columns = [c.upper() for c in out.columns]
    write_pandas(conn, out, table, database=db, schema=schema, overwrite=False)


# ---------------------------------------------------------------------------
# Revue HTML : l'etape de validation humaine
# ---------------------------------------------------------------------------

HTML_HEAD = """<!doctype html><html lang="fr"><head><meta charset="utf-8">
<title>Revue NER transcriptions Aircall</title><style>
body{font-family:-apple-system,Segoe UI,Roboto,sans-serif;margin:0;background:#f5f5f7;color:#1d1d1f}
header{position:sticky;top:0;background:#fff;padding:12px 20px;border-bottom:1px solid #ddd}
.l span{display:inline-block;margin-right:14px;font-size:13px}
.sw{display:inline-block;width:12px;height:12px;border-radius:3px;vertical-align:middle;margin-right:4px}
table{border-collapse:collapse;width:100%;background:#fff}
td,th{padding:8px 12px;border-bottom:1px solid #eee;font-size:13px;text-align:left;vertical-align:top}
th{background:#fafafa;position:sticky;top:46px}
del{background:#fbeaea;text-decoration:line-through;opacity:.6;margin-right:3px}
ins{text-decoration:none;font-weight:600;padding:0 3px;border-radius:3px}
.d-person ins{background:#ffd7dd}.d-address ins{background:#e6d7ff}.d-phone ins{background:#fff3b0}
.n{color:#888;font-variant-numeric:tabular-nums}
</style></head><body>
<header><div class="l">
<span><span class="sw" style="background:#ffd7dd"></span>Prenom / nom</span>
<span><span class="sw" style="background:#e6d7ff"></span>Adresse</span>
<span><span class="sw" style="background:#fff3b0"></span>Telephone</span>
<span class="n">Les tags [PARENT], [ENFANT], [ACCOMPAGNANTE] viennent de la passe dictionnaire.</span>
</div></header>
<table><tr><th>Segments</th><th>Avant / apres</th></tr>
"""


def highlight(text, gardees):
    out, last = [], 0
    for debut, fin, tag, span in gardees:
        out.append(html.escape(text[last:debut]))
        kind = {"[PRENOM_NOM]": "person", "[ADRESSE]": "address",
                "[TELEPHONE]": "phone"}[tag]
        out.append(f'<span class="d d-{kind}"><del>{html.escape(span)}</del>'
                   f'<ins>{html.escape(tag)}</ins></span>')
        last = fin
    out.append(html.escape(text[last:]))
    return "".join(out)


def write_revue(path, lignes):
    with open(path, "w", encoding="utf-8") as f:
        f.write(HTML_HEAD)
        for nb_segments, text, gardees in lignes:
            f.write(f'<tr><td class="n">{nb_segments}</td>'
                    f'<td>{highlight(text, gardees)}</td></tr>')
        f.write("</table></body></html>")


# ---------------------------------------------------------------------------

def run_inference(model, lot, device, etat):
    """
    Un lot a travers GLiNER, avec repli sur CPU si MPS refuse une operation.

    etat porte le peripherique courant : si MPS echoue une fois, on bascule
    definitivement plutot que de retenter a chaque lot.
    """
    import torch
    try:
        with torch.inference_mode():
            return model.inference(
                lot, GLINER_LABELS,
                threshold=GLINER_THRESHOLD, batch_size=len(lot),
            )
    except Exception as ex:
        if etat["device"] != "cpu":
            print(f"  !! {etat['device']} a echoue ({type(ex).__name__}), repli sur CPU")
            model.to("cpu")
            etat["device"] = "cpu"
            try:
                with torch.inference_mode():
                    return model.inference(
                        lot, GLINER_LABELS,
                        threshold=GLINER_THRESHOLD, batch_size=len(lot),
                    )
            except Exception as ex2:
                print(f"  !! lot abandonne : {ex2}")
                return [[] for _ in lot]
        print(f"  !! lot abandonne : {ex}")
        return [[] for _ in lot]


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--limit", type=int,
                    help="ne traiter que les N textes les plus frequents (validation)")
    ap.add_argument("--batch-size", type=int, default=64,
                    help="taille de lot GLiNER (defaut 64 ; MPS aime les gros lots)")
    ap.add_argument("--device", default="auto", choices=["auto", "mps", "cuda", "cpu"],
                    help="peripherique d'inference (defaut auto)")
    ap.add_argument("--threads", type=int,
                    help="threads torch pour le mode CPU (defaut : tous les coeurs)")
    ap.add_argument("--flush-every", type=int, default=4000,
                    help="ecrire dans Snowflake tous les N textes (defaut 4000)")
    ap.add_argument("--revue", action="store_true",
                    help="produire la revue HTML avant/apres")
    ap.add_argument("--dry-run", action="store_true",
                    help="ne rien ecrire dans Snowflake")
    ap.add_argument("--force", action="store_true",
                    help="retraiter meme les textes deja presents dans la table cible")
    args = ap.parse_args()

    global GAZETTEER, COMMON_CORPUS_WORDS

    config = load_sf_config()
    conn = sf_connect(config)

    print(f"Lecture des textes candidats depuis {SOURCE_TABLE}...")
    todo = fetch_todo(conn, limit=args.limit, force=args.force)
    if todo.empty:
        print("Rien a traiter : tous les textes candidats sont deja anonymises.")
        return
    total_chars = int(todo["text_dict"].str.len().sum())
    print(f"  {len(todo)} textes distincts, {total_chars/1e6:.1f} Mo, "
          f"{int(todo['nb_segments'].sum())} segments couverts.")

    GAZETTEER = load_gazetteer(conn)
    print(f"  Liste blanche : {len(GAZETTEER)} noms connus en base.")
    COMMON_CORPUS_WORDS = load_common_words(conn)
    print(f"  Vocabulaire courant deduit du corpus : {len(COMMON_CORPUS_WORDS)} mots.")

    import torch
    torch.set_num_threads(args.threads or os.cpu_count() or 4)
    device = pick_device(args.device)

    print(f"Chargement de GLiNER ({GLINER_MODEL}) sur {device}...")
    from gliner import GLiNER
    model = GLiNER.from_pretrained(GLINER_MODEL)
    model.eval()
    if device != "cpu":
        model.to(device)
    etat = {"device": device}

    # Les lots sont constitues par longueur croissante. Le transformeur pade
    # chaque lot a la longueur de son plus long element : melanger un segment
    # de 40 caracteres avec un de 3 000 fait calculer 98 % de vide. Le tri
    # groupe les tailles voisines et supprime ce gaspillage.
    ordre = todo["text_dict"].str.len().sort_values().index
    todo = todo.loc[ordre].reset_index(drop=True)

    textes = todo["text_dict"].tolist()
    longueurs = [len(t) for t in textes]
    total_car = sum(longueurs)
    resultats, revue, journal = [], [], []
    ecrits = 0
    car_faits = 0
    # Fenetre glissante (temps, caracteres traites) pour l'estimation. Une
    # moyenne depuis le debut serait trompeuse ici : le tri par longueur fait
    # decroitre le debit en continu, donc le passe est toujours plus rapide que
    # l'avenir. Mesure du premier run : 110 textes/s au demarrage, 12 a la fin.
    fenetre = deque(maxlen=12)
    t0 = time.time()
    fenetre.append((t0, 0))
    pas = max(1, args.batch_size)

    def flush(jusqu_a):
        """Ecrit dans Snowflake les textes traites et pas encore pousses."""
        nonlocal ecrits
        if args.dry_run or jusqu_a <= ecrits:
            return
        tranche = todo.iloc[ecrits:jusqu_a].copy()
        tranche["text_ner"] = resultats[ecrits:jusqu_a]
        tranche["nb_tags"] = [len(TAG_RE.findall(t)) for t in tranche["text_ner"]]
        write_results(conn, tranche)
        ecrits = jusqu_a

    for i in range(0, len(textes), pas):
        lot = textes[i:i + pas]
        sorties = run_inference(model, lot, device, etat)

        for j, (texte, entites) in enumerate(zip(lot, sorties)):
            anonymise, gardees = anonymize(texte, entites)
            resultats.append(anonymise)
            nb_segments = int(todo["nb_segments"].iloc[i + j])
            if gardees:
                revue.append((nb_segments, texte, gardees))
                for _, _, tag, span in gardees:
                    journal.append([tag, span, nb_segments])

        fait = len(resultats)
        if fait - ecrits >= args.flush_every:
            flush(fait)
            print(f"  -> {ecrits} textes ecrits dans {TARGET_TABLE}")

        car_faits = sum(longueurs[:fait])
        if (i // pas) % 20 == 0 or fait == len(textes):
            maintenant = time.time()
            fenetre.append((maintenant, car_faits))
            ecoule = maintenant - t0
            # Debit en caracteres sur la fenetre : plus stable que le nombre de
            # textes, dont le cout varie d'un facteur cinq entre debut et fin.
            t_vieux, car_vieux = fenetre[0]
            dt = max(maintenant - t_vieux, 0.001)
            car_par_s = max((car_faits - car_vieux) / dt, 1.0)
            reste = (total_car - car_faits) / car_par_s
            print(f"  {fait}/{len(textes)}  {ecoule/60:.1f} min  "
                  f"{car_par_s/1000:.1f}k car/s  "
                  f"~{reste/60:.0f} min restantes  {len(journal)} remplacements",
                  flush=True)

    todo["text_ner"] = resultats
    todo["nb_tags"] = [len(TAG_RE.findall(t)) for t in resultats]
    modifies = int((todo["text_ner"] != todo["text_dict"]).sum())

    ecoule = time.time() - t0
    print(f"\n{len(journal)} remplacements sur {modifies} textes "
          f"({modifies/len(todo)*100:.1f} %), en {ecoule/60:.1f} min "
          f"({len(textes)/max(ecoule,0.001):.1f} textes/s sur {etat['device']}).")

    if args.revue and revue:
        revue.sort(key=lambda r: -r[0])
        path = os.path.join(BASE, "revue_aircall_ner.html")
        write_revue(path, revue)
        print(f"  Revue HTML  : {path}")
    if journal:
        path = os.path.join(BASE, "rapport_aircall_ner.csv")
        with open(path, "w", encoding="utf-8", newline="") as f:
            w = csv.writer(f)
            w.writerow(["tag", "original", "nb_segments_concernes"])
            w.writerows(journal)
        print(f"  Journal CSV : {path}")

    if args.dry_run:
        print("--dry-run : rien n'a ete ecrit dans Snowflake.")
    else:
        flush(len(resultats))
        print(f"  {ecrits} lignes au total dans {TARGET_TABLE}.")
        print("\nRelancer ensuite :")
        print("  dbt run -s aircall_transcriptions_anonymised+ --profiles-dir dbt")

    conn.close()


if __name__ == "__main__":
    main()
