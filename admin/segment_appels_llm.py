"""
Notation des appels d'accompagnement par le modele -- la grille a six criteres.

Applique admin/prompts/segmentation_appel.md a chaque transcription anonymisee
et ecrit une ligne par appel dans MOTS_DW.FACT.AIRCALL_SEGMENTATION, que
fact/aircall_analysis joint sur call_id. C'est la suite industrialisee des 300
notes posees a la main dans dbt/seeds/segmentation_appel.csv : meme grille,
memes colonnes, meme table d'arrivee apres --import-seed.

Deux moteurs, meme grille, meme table :

  * --engine claude-code (defaut) : une invocation `claude -p` par appel, payee
    par l'abonnement Claude. Pas de cle d'API. Le quota et le temps de mur sont
    les limites ; une passe interrompue se reprend en relancant la commande.
  * --engine api : le Batch API, moitie prix, tout le corpus en moins de 24 h.
    L'identifiant de lot est journalise dans admin/logs/segmentation_batches.json,
    donc une passe interrompue se recupere avec --collect sans rien repayer.

Dans les deux cas, un contexte neuf par appel : c'est ce qui empeche la note
d'un appel d'influencer celle du suivant.

Trois choix structurants :

  * la source est fact.aircall_transcriptions_anonymised_by_call, filtree sur
    is_anonymisation_complete. Un appel dont un seul segment attend encore la
    passe NER n'est PAS envoye : le texte sortirait du perimetre d'acces de la
    source avec des prenoms de tiers encore en clair.
  * la sortie est du JSON, pas une ligne CSV produite par le modele. Les
    verbatims contiennent des `;` et des guillemets ; c'etait le maillon faible
    du format d'origine.
  * score et flag sont RECALCULES a partir de c1..c6 cote script. Ce sont des
    fonctions deterministes des six notes ; les laisser au modele ajoutait une
    source d'erreur sans rien apporter. Le compteur d'ecarts est affiche en fin
    de passe -- un taux qui grimpe signale un modele qui lit mal la grille.

Usage :
    python admin/segment_appels_llm.py --import-seed            # reprendre les 300 notes manuelles
    python admin/segment_appels_llm.py --probe                  # un appel de controle, commande imprimee
    python admin/segment_appels_llm.py --estimate               # quota et temps, sans rien lancer
    python admin/segment_appels_llm.py --only-notes --limit 30  # calibrage : re-noter des appels deja notes
    python admin/segment_appels_llm.py --limit 500 --workers 3  # passe reelle
    python admin/segment_appels_llm.py --engine api --collect   # recuperer les lots API en attente

Dependances :
    pip install snowflake-connector-python pandas pyyaml
    le CLI claude (curl -fsSL https://claude.ai/install.sh | bash), deja
    authentifie s'il partage les identifiants de l'extension VS Code
    pip install anthropic  +  ANTHROPIC_API_KEY, pour --engine api seulement
"""

import argparse
import hashlib
import json
import os
import re
import sys
import time
from datetime import datetime, timezone

import pandas as pd

from sf_conn import sf_connect

BASE = os.path.dirname(os.path.abspath(__file__))
PROMPT_PATH = os.path.join(BASE, "prompts", "segmentation_appel.md")
STATE_PATH = os.path.join(BASE, "logs", "segmentation_batches.json")

SOURCE_TABLE = "MOTS_DW.FACT.AIRCALL_TRANSCRIPTIONS_ANONYMISED_BY_CALL"
TARGET_TABLE = "MOTS_DW.FACT.AIRCALL_SEGMENTATION"
SEED_TABLE = "MOTS_DW.seed.segmentation_appel"

MODELE_DEFAUT = "claude-opus-5"
MAX_TOKENS = 8000

# Un appel reel, par opposition a un repondeur ou un decroche-raccroche. Le
# seuil est celui de fact/aircall_analysis (is_real_call), pas une invention
# locale : les deux doivent designer le meme sous-ensemble.
DUREE_MINI = 400

# Caracteres de transcription par minute d'appel. Un appel transcrit normalement
# en porte 865 ; en dessous de 150, la transcription a echoue -- 297 appels du
# corpus tiennent en 5 segments et 176 caracteres pour 19 minutes de
# conversation, parfois un seul segment « P: First. ». Les noter couterait le
# prix d'une transcription complete pour produire six zeros qui ne disent rien
# de l'accompagnante. 367 appels sur 6 423 sont ecartes a ce titre, soit 5,7 %.
DENSITE_MINI = 150

# Au-dela, un lot approche les plafonds de l'API (100 000 requetes ou 256 Mo).
# 2 000 appels x 16 ko font 32 Mo : large marge, et un lot qui echoue ne coute
# pas toute la passe.
TAILLE_LOT = 2000

# $ par million de jetons (entree, sortie), tarif premier plan. Le Batch API
# applique -50 % sur les deux.
PRIX = {
    "claude-opus-5": (5.0, 25.0),
    "claude-sonnet-5": (2.0, 10.0),
    "claude-haiku-4-5": (1.0, 5.0),
}
REMISE_BATCH = 0.5

MARQUEUR = "<!-- === MESSAGE UTILISATEUR === -->"
COMMENTAIRE = re.compile(r"<!--.*?-->", re.S)

NOTES = ["c1", "c2", "c3", "c4", "c5", "c6"]
DECOMPTES = ["nb_q_ouvertes", "nb_q_fermees", "nb_reformulations"]
ORIGINES_PM = ["parent", "co_construite", "accompagnante", "aucune_PM"]
FLAGS = ["EXCELLENT", "MOYEN", "A_AMELIORER"]

# L'accord d'enregistrement est la condition d'acces a la grille : sans lui, on
# ne note rien. Mesure a l'appui, la polarite de la question s'inverse d'une
# accompagnante a l'autre -- 3 235 « vous m'autorisez » contre 129 « ca vous
# derange » -- et dans le second cas « non » vaut accord. C'est la raison pour
# laquelle ce jugement est confie au modele et non a une regex : « non, ca ne me
# derange pas » est un accord que tout classificateur par mots-cles compte comme
# un refus.
# Deux valeurs, pas quatre. Distinguer le refus de la demande jamais faite et de
# la reponse ambigue invitait le modele a qualifier une absence -- un jugement de
# plus, sur l'element le moins observable de la transcription. Le besoin est
# binaire : a-t-on le droit d'analyser cet appel, oui ou non. En cas de doute,
# c'est non.
ACCORDS = ["accord", "pas_d_accord"]

# Flag porte par un appel qu'on s'interdit de noter. Ce n'est pas une mauvaise
# note : c'est l'absence de droit d'analyser.
FLAG_SANS_ACCORD = "SANS_ACCORD"

# Les colonnes de la table cible, dans l'ordre. Les six premieres reprennent
# exactement l'en-tete du seed manuel, pour que les deux jeux de notes
# s'empilent sans traduction.
COLONNES = (
    ["id_appel", "numero_appel", "accord_enregistrement", "accord_verbatim",
     "flag", "score"] + NOTES + DECOMPTES
    + ["origine_pm", "manques", "verbatim_cle",
       "origine_note", "modele", "prompt_hash", "effort", "moteur",
       "regle_flag"]
)

# Tout ce qui suit l'accord est nullable : quand l'accord manque, le modele doit
# pouvoir rendre un objet ou seuls les deux premiers champs sont remplis.
SCHEMA = {
    "type": "object",
    "properties": {
        "accord_enregistrement": {"type": "string", "enum": ACCORDS},
        "accord_verbatim": {"type": "string", "maxLength": 400},
        **{n: {"type": ["integer", "null"], "enum": [0, 1, 2, None]} for n in NOTES},
        "score": {"type": ["integer", "null"], "minimum": 0, "maximum": 12},
        "flag": {"type": ["string", "null"], "enum": FLAGS + [None]},
        **{d: {"type": ["integer", "null"], "minimum": 0} for d in DECOMPTES},
        "origine_pm": {"type": ["string", "null"], "enum": ORIGINES_PM + [None]},
        "manques": {
            "type": ["array", "null"],
            "items": {"type": "string", "maxLength": 60},
            "maxItems": 3,
        },
        "verbatim_cle": {"type": ["string", "null"], "maxLength": 200},
    },
    "required": ["accord_enregistrement", "accord_verbatim"] + NOTES
                + ["score", "flag"] + DECOMPTES
                + ["origine_pm", "manques", "verbatim_cle"],
    "additionalProperties": False,
}


# ---------------------------------------------------------------------------
# Le prompt
# ---------------------------------------------------------------------------

def charger_prompt():
    """
    Coupe le fichier de prompt en (systeme, gabarit utilisateur, empreinte).

    Les commentaires HTML sont retires : ce sont des notes pour nous, pas des
    instructions pour le modele, et ils useraient du cache pour rien.

    L'empreinte ne porte que sur la partie systeme -- la grille. C'est elle qui
    definit ce que vaut une note ; le gabarit utilisateur ne fait que porter la
    transcription.
    """
    with open(PROMPT_PATH, encoding="utf-8") as f:
        brut = f.read()
    if MARQUEUR not in brut:
        sys.exit(f"Marqueur {MARQUEUR!r} absent de {PROMPT_PATH}")
    systeme, gabarit = brut.split(MARQUEUR, 1)
    systeme = COMMENTAIRE.sub("", systeme).strip()
    gabarit = COMMENTAIRE.sub("", gabarit).strip()
    empreinte = hashlib.md5(systeme.encode("utf-8")).hexdigest()[:8]
    return systeme, gabarit, empreinte


def rendre(gabarit, appel):
    numero = appel["call_session"]
    return (gabarit
            .replace("{{ID_APPEL}}", str(appel["call_id"]))
            .replace("{{NUMERO_APPEL}}",
                     "NA" if numero in (None, 0) else str(int(numero)))
            .replace("{{TRANSCRIPTION}}", appel["transcript"]))


# ---------------------------------------------------------------------------
# Snowflake
# ---------------------------------------------------------------------------

def creer_table(conn):
    conn.cursor().execute(
        f"""create table if not exists {TARGET_TABLE} (
                id_appel              number,
                numero_appel          number,
                accord_enregistrement varchar,
                accord_verbatim       varchar,
                flag                  varchar,
                score             number,
                c1 number, c2 number, c3 number,
                c4 number, c5 number, c6 number,
                nb_q_ouvertes     number,
                nb_q_fermees      number,
                nb_reformulations number,
                origine_pm        varchar,
                manques           varchar,
                verbatim_cle      varchar,
                origine_note      varchar,
                modele            varchar,
                prompt_hash       varchar,
                effort            varchar,
                moteur            varchar,
                regle_flag        varchar,
                date_traite       timestamp_ntz default current_timestamp()
            )"""
    )


def fetch_todo(conn, limit=None, sessions=None, duree_mini=DUREE_MINI,
               densite_mini=DENSITE_MINI, only_notes=False, force=False,
               alea=False, graine=None, un_par_session=True):
    """
    Les appels notables et pas encore notes.

    L'anti-jointure sur la table cible est ce qui rend les relances bon marche :
    apres une passe complete, un run hebdomadaire ne voit que les appels des
    nouvelles cohortes.

    En mode --only-notes on retient au contraire les appels DEJA notes a la
    main, en excluant ceux que le modele a deja vus : c'est l'echantillon de
    calibrage, celui sur lequel l'accord modele/humain se mesure.
    """
    conditions = [f"duration > {int(duree_mini)}", "is_anonymisation_complete"]
    densite = f"length(transcript) / (duration / 60.0) >= {float(densite_mini)}"

    if sessions:
        liste = ", ".join(str(int(s)) for s in sessions)
        conditions.append(f"call_session in ({liste})")

    # Un echantillon aleatoire plutot que les plus recents : pour juger la
    # grille, il faut couvrir toutes les accompagnantes et toutes les periodes,
    # pas la derniere cohorte. La graine rend le tirage reproductible.
    ordre = (f"order by hash(call_id, {int(graine)})" if alea and graine is not None
             else "order by random()" if alea
             else "order by date_started desc")

    # Les appels ecartes pour transcription trop maigre sont comptes a part :
    # un filtre silencieux qui mange 6 % du corpus est un filtre qu'on oublie.
    cur = conn.cursor()
    cur.execute(f"""select count(*) from {SOURCE_TABLE}
                    where {' and '.join(conditions)} and not ({densite})""")
    ecartes = cur.fetchone()[0]

    # Un seul appel par famille et par session : le plus long.
    #
    # Une session compte souvent plusieurs appels -- rappels, lignes coupees,
    # messages. Le vrai entretien est le plus long ; les autres en sont des
    # morceaux. Noter un morceau donne une note severe qui ne dit rien de
    # l'accompagnante, et aircall_analysis ne l'affichera meme pas : ce modele
    # joint sur max_duration, donc il ne retient que le plus long. Sur la
    # premiere passe, 12 notes sur 99 portaient sur un appel de rang 2 -- du
    # quota depense pour des lignes que personne ne lirait.
    #
    # /!\ Ce choix se fait AVANT l'anti-jointure. Sinon, une session dont le
    # plus long appel est deja note ferait remonter le deuxieme, et le probleme
    # reviendrait par la fenetre.
    selection = ("qualify row_number() over (partition by family_id, call_session "
                 "order by duration desc, call_id) = 1") if un_par_session else ""
    if un_par_session:
        conditions.append("family_id is not null")

    apres_coup = []
    if only_notes:
        apres_coup.append(f"call_id in (select id_appel from {SEED_TABLE})")
        if not force:
            apres_coup.append(
                f"call_id not in (select id_appel from {TARGET_TABLE}"
                f" where origine_note = 'llm')")
    elif not force:
        apres_coup.append(f"call_id not in (select id_appel from {TARGET_TABLE})")

    sql = f"""
        with candidats as (
            select call_id, family_id, call_session, duration, nb_segments,
                   length(transcript) as nb_car, transcript, date_started
            from {SOURCE_TABLE}
            where {' and '.join(conditions + [densite])}
            {selection}
        )
        select call_id, call_session, duration, nb_segments, nb_car, transcript
        from candidats
        {('where ' + ' and '.join(apres_coup)) if apres_coup else ''}
        {ordre}
        {f'limit {int(limit)}' if limit else ''}
    """
    cur.execute(sql)
    colonnes = [d[0].lower() for d in cur.description]
    return pd.DataFrame(cur.fetchall(), columns=colonnes), ecartes


def ecrire(conn, lignes):
    """
    Ecrit les notes dans la table cible.

    date_traite n'est pas fournie : la valeur par defaut de la colonne la pose
    cote entrepot. C'est volontaire -- write_pandas pousse les horodatages
    pandas en nanosecondes dans un timestamp_ntz, ce qui a produit des dates en
    l'an 56765733 dans aircall_transcriptions_ner.
    """
    from snowflake.connector.pandas_tools import write_pandas

    db, schema, table = TARGET_TABLE.split(".")
    out = pd.DataFrame(lignes, columns=COLONNES)
    out.columns = [c.upper() for c in out.columns]
    ok, _, nb, _ = write_pandas(conn, out, table, database=db, schema=schema,
                                overwrite=False, quote_identifiers=False)
    if not ok:
        sys.exit("write_pandas a echoue.")
    return nb


def reetiqueter(conn):
    """
    Recalcule flag et score de toutes les lignes, sous la regle courante.

    Le flag est une fonction deterministe de C1..C6 : en changer la definition
    ne demande pas de renoter un seul appel, juste de repasser ici. C'est la
    raison d'etre du recalcul cote script -- si le modele avait pose le flag
    lui-meme, changer le seuil aurait coute une passe complete.

    Les notes manuelles sont reetiquetees aussi : sinon la colonne melangerait
    deux regles et ne voudrait plus rien dire d'une ligne a l'autre.
    """
    cur = conn.cursor()
    cur.execute(f"select id_appel, c1, c2, c3, c4, c5, c6, flag, regle_flag "
                f"from {TARGET_TABLE} "
                f"where c1 is not null and c6 is not null")
    lignes = cur.fetchall()

    changements, par_regle = [], {}
    for id_appel, *notes_brutes, flag_actuel, regle in lignes:
        note = dict(zip(NOTES, [int(n) for n in notes_brutes]))
        score, flag, _ = recalculer(note)
        par_regle[regle] = par_regle.get(regle, 0) + 1
        if flag != flag_actuel or regle != REGLE_FLAG:
            changements.append((flag, score, REGLE_FLAG, int(id_appel)))

    if not changements:
        print(f"Rien a reetiqueter : tout est deja sous la regle {REGLE_FLAG}.")
        return 0

    print(f"Regles en place avant : {par_regle}")

    # Un seul UPDATE, pas un par ligne : en executemany, Snowflake paie un
    # aller-retour par instruction -- 400 lignes prenaient plusieurs minutes, et
    # une passe complete en prendrait une demi-heure. La liste de valeurs est
    # jointe cote entrepot.
    #
    # La regle reste ecrite une seule fois, en Python. La traduire en CASE SQL
    # aurait mis deux copies en circulation, et c'est exactement comme ca que la
    # stop-list du NER a derive jusqu'a ce que GLiNER prenne « Allo » pour un
    # prenom.
    valeurs = ", ".join(f"('{f}', {sc}, '{r}', {i})" for f, sc, r, i in changements)
    cur.execute(f"""
        update {TARGET_TABLE} as t
        set flag = v.flag, score = v.score, regle_flag = v.regle
        from (values {valeurs}) as v(flag, score, regle, id_appel)
        where t.id_appel = v.id_appel
    """)
    print(f"{len(changements)} lignes reetiquetees sous {REGLE_FLAG}.")
    return len(changements)


def importer_seed(conn):
    """Verse les notes manuelles dans la table, une fois, avant la bascule."""
    cur = conn.cursor()
    cur.execute(f"select count(*) from {TARGET_TABLE} where origine_note = 'manuel'")
    if cur.fetchone()[0]:
        print("Les notes manuelles sont deja dans la table : rien a faire.")
        return 0

    cur.execute(f"select * from {SEED_TABLE}")
    colonnes = [d[0].lower() for d in cur.description]
    lignes = []
    for brut in cur.fetchall():
        row = dict(zip(colonnes, brut))
        numero = str(row.get("numero_appel") or "").strip()
        lignes.append({
            "id_appel": int(row["id_appel"]),
            "numero_appel": int(numero) if numero.isdigit() else None,
            "accord_enregistrement": None,
            "accord_verbatim": None,
            "flag": row["flag"],
            "score": row["score"],
            **{n: row[n] for n in NOTES},
            **{d: row[d] for d in DECOMPTES},
            "origine_pm": row["origine_pm"],
            "manques": row["manques"],
            "verbatim_cle": row["verbatim_cle"],
            "origine_note": "manuel",
            "modele": None,
            "prompt_hash": None,
            "effort": None,
            "moteur": None,
            "regle_flag": REGLE_FLAG,
        })
    nb = ecrire(conn, lignes)
    print(f"{nb} notes manuelles reprises depuis {SEED_TABLE}.")
    return nb


# ---------------------------------------------------------------------------
# Notation
# ---------------------------------------------------------------------------

# Version de la regle de flag. Le flag est une fonction deterministe de C1..C6 :
# le changer ne demande pas de renoter, seulement de relancer --reflag. Cette
# etiquette dit sous quelle regle chaque ligne a ete etiquetee.
#
# R0 (2026-10-06, abandonnee) : EXCELLENT si score >= 10 et C2 = 2 et C6 = 2 ;
#   A_AMELIORER si score <= 5 OU C2 = 0 OU C6 = 0. Mesuree sur 399 notes, elle
#   mettait 64 a 66 % des appels en A_AMELIORER et 1 a 4 % en EXCELLENT : le
#   flag ne distinguait plus rien et disait surtout « C2 ou C6 vaut 0 », ce que
#   les colonnes de criteres disent deja.
# R1 (actuelle) : il faut que C2 ET C6 echouent tous les deux pour basculer en
#   A_AMELIORER. La regle metier est preservee -- un appel a PM dictee ne peut
#   pas etre EXCELLENT -- mais elle ne confisque plus les trois quarts du
#   corpus. Donne 33-37 % / 45-52 % / 10-21 %.
REGLE_FLAG = "R1"


def recalculer(note):
    """
    Score et flag deduits des six notes, en appliquant la grille a la lettre.

    Renvoie (score, flag, ecart) ou ecart dit si le modele avait repondu autre
    chose. Le flag se lit dans l'ordre, EXCELLENT d'abord.
    """
    score = sum(int(note[n]) for n in NOTES)
    c2, c6 = int(note["c2"]), int(note["c6"])
    if score >= 9 and c2 >= 1 and c6 >= 1:
        flag = "EXCELLENT"
    elif score <= 4 or (c2 == 0 and c6 == 0):
        flag = "A_AMELIORER"
    else:
        flag = "MOYEN"
    ecart = (int(note.get("score", -1)) != score) or (note.get("flag") != flag)
    return score, flag, ecart


def note_vide(note):
    """
    L'appel n'a pas l'accord d'enregistrement : il n'y a rien a noter.

    On accepte aussi le cas ou le modele dit `accord` mais ne rend aucun
    critere : plutot que d'inventer des zeros, on traite la ligne comme non
    notee. Un zero se melerait aux moyennes et ferait passer pour mauvaise une
    accompagnante qu'on n'a pas le droit d'evaluer.
    """
    return (note.get("accord_enregistrement") != "accord"
            or any(note.get(n) is None for n in NOTES))


def ligne_resultat(appel_id, numero, note, modele, empreinte, effort, moteur):
    vide = note_vide(note)
    score, flag = (None, FLAG_SANS_ACCORD) if vide else recalculer(note)[:2]
    manques = [m.strip() for m in (note.get("manques") or []) if m and m.strip()]
    return {
        "id_appel": int(appel_id),
        "numero_appel": None if numero in (None, 0) else int(numero),
        "accord_enregistrement": note.get("accord_enregistrement"),
        "accord_verbatim": (None if vide
                            else (note.get("accord_verbatim") or "").strip() or None),
        "flag": flag,
        "score": score,
        **{n: None if vide else int(note[n]) for n in NOTES},
        **{d: None if vide else int(note[d]) for d in DECOMPTES},
        "origine_pm": None if vide else note["origine_pm"],
        "manques": None if vide else ("|".join(manques) or None),
        "verbatim_cle": None if vide else ((note.get("verbatim_cle") or "").strip() or None),
        "origine_note": "llm",
        "modele": modele,
        "prompt_hash": empreinte,
        "effort": effort,
        "moteur": moteur,
        "regle_flag": REGLE_FLAG,
    }


def params_requete(systeme, user, modele, effort):
    from anthropic.types.message_create_params import MessageCreateParamsNonStreaming

    return MessageCreateParamsNonStreaming(
        model=modele,
        max_tokens=MAX_TOKENS,
        # La grille est identique d'un appel a l'autre et vient avant la
        # transcription : c'est le prefixe a mettre en cache.
        system=[{"type": "text", "text": systeme,
                 "cache_control": {"type": "ephemeral"}}],
        messages=[{"role": "user", "content": user}],
        thinking={"type": "adaptive"},
        output_config={
            "effort": effort,
            "format": {"type": "json_schema", "schema": SCHEMA},
        },
    )


def texte_json(message):
    """Le premier bloc texte d'une reponse, que output_config garantit JSON."""
    for bloc in message.content:
        if bloc.type == "text":
            return bloc.text
    raise ValueError(f"aucun bloc texte (stop_reason={message.stop_reason})")


def cumuler_usage(total, usage):
    total["entree"] += usage.input_tokens or 0
    total["sortie"] += usage.output_tokens or 0
    total["cache_lu"] += getattr(usage, "cache_read_input_tokens", 0) or 0
    total["cache_ecrit"] += getattr(usage, "cache_creation_input_tokens", 0) or 0
    return total


def cout(total, modele, batch=True):
    prix_e, prix_s = PRIX.get(modele, PRIX[MODELE_DEFAUT])
    remise = REMISE_BATCH if batch else 1.0
    # Un jeton lu en cache coute 10 % du tarif d'entree, un jeton ecrit 125 %.
    return remise * (
        (total["entree"] * prix_e
         + total["cache_lu"] * prix_e * 0.10
         + total["cache_ecrit"] * prix_e * 1.25
         + total["sortie"] * prix_s) / 1e6
    )


# ---------------------------------------------------------------------------
# Journal des lots
# ---------------------------------------------------------------------------

def lire_etat():
    if not os.path.exists(STATE_PATH):
        return []
    with open(STATE_PATH) as f:
        return json.load(f)


def ecrire_etat(lots):
    os.makedirs(os.path.dirname(STATE_PATH), exist_ok=True)
    with open(STATE_PATH, "w") as f:
        json.dump(lots, f, indent=2)


def noter_lot(lot):
    lots = lire_etat()
    lots.append(lot)
    ecrire_etat(lots)


def marquer_recupere(batch_id, nb_ecrites):
    lots = lire_etat()
    for lot in lots:
        if lot["batch_id"] == batch_id:
            lot["recupere_le"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
            lot["nb_ecrites"] = nb_ecrites
    ecrire_etat(lots)


# ---------------------------------------------------------------------------
# Les trois modes d'appel : estimation, lot, synchrone
# ---------------------------------------------------------------------------

def estimer(client, todo, systeme, gabarit, modele, effort, echantillon=20):
    """
    Compte les jetons d'entree sur un echantillon et extrapole.

    count_tokens est gratuit et exact sur l'entree. La sortie, elle, ne se
    devine pas : la reflexion du modele en represente l'essentiel et depend de
    l'effort demande. On affiche donc une fourchette, bornee par deux
    hypotheses explicites, et on renvoie au calibrage pour la vraie mesure.
    """
    sous = todo.head(echantillon)
    jetons = []
    for _, appel in sous.iterrows():
        rep = client.messages.count_tokens(
            model=modele,
            system=[{"type": "text", "text": systeme}],
            messages=[{"role": "user", "content": rendre(gabarit, appel)}],
        )
        jetons.append(rep.input_tokens)

    moyen = sum(jetons) / len(jetons)
    prix_e, prix_s = PRIX.get(modele, PRIX[MODELE_DEFAUT])
    n = len(todo)
    entree = moyen * n
    print(f"\nEstimation sur {len(sous)} appels, extrapolee a {n} :")
    print(f"  entree      {moyen:,.0f} jetons/appel  -> {entree/1e6:,.1f} M jetons")
    print(f"  cout entree {REMISE_BATCH * entree * prix_e / 1e6:,.2f} $ (Batch API, -50 %)")
    for sortie_par_appel in (300, 1200, 2500):
        sortie = sortie_par_appel * n
        total = REMISE_BATCH * (entree * prix_e + sortie * prix_s) / 1e6
        print(f"  + {sortie_par_appel:>5} jetons de sortie/appel -> {total:,.2f} $ au total")
    print("\n  La sortie utile fait ~250 jetons ; le reste est de la reflexion.")
    print("  Mesurer pour de vrai : --only-notes --limit 30 (le cout reel est")
    print("  affiche en fin de passe).")


def estimer_abonnement(todo, systeme, workers, secondes_par_appel=45):
    """
    Ce que la passe coute sur un abonnement : des jetons de quota et du temps.

    Pas de prix : un abonnement ne facture pas a l'appel. Les deux grandeurs qui
    decident sont le quota (fenetres de 5 h et hebdomadaire) et le temps de mur,
    puisque chaque appel est une invocation sequentielle de plus.

    Le compte de jetons est deduit des caracteres, avec le ratio MESURE sur la
    passe du 2026-10-06 : 1,9 caractere par jeton, overhead du CLI compris.
    L'estimation naive (3,8 car/jeton, le ratio courant du francais ecrit) se
    trompait d'un facteur deux -- la transcription parlee, hachee en centaines de
    tours de parole prefixes et semee de balises [PRENOM_NOM], tokenise beaucoup
    moins bien qu'un texte suivi.

    Ca reste une estimation. Le chiffre qui vaut est celui qu'imprime une vraie
    passe, et --probe le donne en quinze secondes.
    """
    car = todo["nb_car"].sum() + len(systeme) * len(todo)
    jetons = car / 1.9
    minutes = len(todo) * secondes_par_appel / 60 / max(workers, 1)
    print(f"\nSur abonnement, {len(todo)} appels representent :")
    print(f"  ~{jetons/1e6:.1f} M jetons d'entree ({jetons/max(len(todo),1):,.0f} par appel, "
          f"grille et overhead du CLI compris)")
    print(f"  ~{1490 * len(todo)/1e6:.1f} M jetons de sortie "
          f"(~1 490 par appel, dont 97 % de reflexion -- mesure)")
    print(f"  ~{minutes/60:.1f} h de temps de mur a {workers} en parallele, "
          f"hors attente de quota")
    print("\n  Le quota se compte par fenetres de 5 h et par semaine : une passe de")
    print("  cette taille sera interrompue plusieurs fois. C'est sans gravite --")
    print("  relancer la meme commande reprend ou la precedente s'est arretee.")
    print("  Commencer par --probe, puis --only-notes --limit 30.")


def soumettre(client, todo, systeme, gabarit, modele, effort, empreinte):
    """Decoupe le travail en lots, les soumet, journalise les identifiants."""
    from anthropic.types.messages.batch_create_params import Request

    lots = []
    for debut in range(0, len(todo), TAILLE_LOT):
        tranche = todo.iloc[debut:debut + TAILLE_LOT]
        requetes = [
            Request(
                custom_id=f"appel-{int(appel['call_id'])}",
                params=params_requete(systeme, rendre(gabarit, appel), modele, effort),
            )
            for _, appel in tranche.iterrows()
        ]
        lot = client.messages.batches.create(requests=requetes)
        numeros = {int(a["call_id"]): (a["call_session"] if a["call_session"] else None)
                   for _, a in tranche.iterrows()}
        noter_lot({
            "batch_id": lot.id,
            "soumis_le": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "nb": len(requetes),
            "modele": modele,
            "effort": effort,
            "prompt_hash": empreinte,
            "numeros_appel": {str(k): v for k, v in numeros.items()},
        })
        lots.append(lot.id)
        print(f"  lot {lot.id} soumis : {len(requetes)} appels")
    return lots


def attendre(client, batch_id, intervalle=30):
    while True:
        lot = client.messages.batches.retrieve(batch_id)
        if lot.processing_status == "ended":
            return lot
        c = lot.request_counts
        print(f"  {batch_id} : {lot.processing_status}, "
              f"{c.succeeded} faits / {c.processing} en cours / {c.errored} en erreur",
              flush=True)
        time.sleep(intervalle)


def collecter(client, conn, batch_id, lot_journal):
    """
    Recupere un lot termine et ecrit ses notes.

    Les resultats arrivent dans un ordre quelconque : la cle est custom_id, pas
    la position.
    """
    modele = lot_journal.get("modele", MODELE_DEFAUT)
    effort = lot_journal.get("effort")
    empreinte = lot_journal.get("prompt_hash")
    numeros = lot_journal.get("numeros_appel", {})

    lignes, echecs, ecarts = [], [], 0
    total = {"entree": 0, "sortie": 0, "cache_lu": 0, "cache_ecrit": 0}

    for res in client.messages.batches.results(batch_id):
        appel_id = res.custom_id.removeprefix("appel-")
        if res.result.type != "succeeded":
            detail = getattr(res.result, "error", None)
            echecs.append((appel_id, res.result.type,
                           getattr(detail, "type", "") if detail else ""))
            continue

        message = res.result.message
        cumuler_usage(total, message.usage)
        if message.stop_reason in ("refusal", "max_tokens"):
            echecs.append((appel_id, message.stop_reason, ""))
            continue
        try:
            note = json.loads(texte_json(message))
        except (ValueError, json.JSONDecodeError) as e:
            echecs.append((appel_id, "json_illisible", str(e)[:60]))
            continue

        ecarts += bool(not note_vide(note) and recalculer(note)[2])
        lignes.append(ligne_resultat(appel_id, numeros.get(str(appel_id)),
                                     note, modele, empreinte, effort, "api"))

    nb = ecrire(conn, lignes) if lignes else 0
    marquer_recupere(batch_id, nb)

    print(f"\n{nb} notes ecrites dans {TARGET_TABLE} pour le lot {batch_id}.")
    if ecarts:
        print(f"  {ecarts} appels ou le score ou le flag du modele differait du "
              f"recalcul ({ecarts/max(len(lignes),1)*100:.0f} %) : c'est le "
              f"recalcul qui est stocke.")
    if echecs:
        print(f"  {len(echecs)} echecs :")
        for appel_id, genre, detail in echecs[:10]:
            print(f"    {appel_id} : {genre} {detail}")
        if len(echecs) > 10:
            print(f"    ... et {len(echecs) - 10} autres")
    print(f"  jetons : {total['entree']:,} entree, {total['cache_lu']:,} lus en cache, "
          f"{total['sortie']:,} sortie")
    print(f"  cout du lot : {cout(total, modele):,.2f} $ "
          f"({cout(total, modele)/max(nb,1):.3f} $/appel)")
    return lignes


def noter_en_direct(client, conn, todo, systeme, gabarit, modele, effort, empreinte):
    """Hors lot : plein tarif, reponse immediate. Pour quelques appels a l'oeil."""
    lignes, ecarts = [], 0
    total = {"entree": 0, "sortie": 0, "cache_lu": 0, "cache_ecrit": 0}
    for i, (_, appel) in enumerate(todo.iterrows(), 1):
        message = client.messages.create(
            **params_requete(systeme, rendre(gabarit, appel), modele, effort))
        cumuler_usage(total, message.usage)
        note = json.loads(texte_json(message))
        ecarts += bool(not note_vide(note) and recalculer(note)[2])
        ligne = ligne_resultat(appel["call_id"], appel["call_session"],
                               note, modele, empreinte, effort, "api")
        lignes.append(ligne)
        print(f"  {i}/{len(todo)}  appel {ligne['id_appel']} : "
              f"{ligne['flag']} {ligne['score']}/12  "
              f"C{''.join(str(ligne[n]) for n in NOTES)}", flush=True)

    nb = ecrire(conn, lignes) if lignes else 0
    print(f"\n{nb} notes ecrites dans {TARGET_TABLE}.")
    if ecarts:
        print(f"  {ecarts} ecarts score/flag recalcules.")
    print(f"  cout : {cout(total, modele, batch=False):,.2f} $ (hors lot, plein tarif)")
    return lignes


# ---------------------------------------------------------------------------
# Moteur Claude Code : l'abonnement au lieu de l'API
#
# Une invocation de `claude -p` par appel, donc un contexte neuf a chaque fois.
# Ce n'est pas qu'une commodite : c'est la seule facon de ne pas laisser la note
# d'un appel influencer celle du suivant. Les 300 notes manuelles ont ete posees
# comme ca, un appel a la fois.
#
# /!\ Les options de `claude` ci-dessous sont celles de la documentation, pas
# d'un essai sur cette machine (le CLI n'y etait pas installe au moment de
# l'ecriture). D'ou --probe : un appel de controle qui imprime la commande
# exacte et ce qu'elle a renvoye, avant d'en lancer des milliers.
# ---------------------------------------------------------------------------

DIRECTIVE = ("Note cet appel selon la grille du prompt systeme. "
             "Reponds par le seul objet JSON demande, sans rien autour.")


def construire_commande(systeme, modele, binaire, schema=False, extra=(),
                        outils=False):
    """
    La commande, une fois par appel.

    Deux choix qui pesent lourd sur le quota et sur la qualite :

      * --system-prompt REMPLACE le prompt systeme de Claude Code au lieu de s'y
        ajouter. Ce qu'on veut n'est pas un assistant de code auquel on annexe
        une grille, c'est un correcteur d'appels. Ca supprime aussi des milliers
        de jetons d'instructions hors sujet, payes a chaque invocation puisque
        chaque `claude -p` est un processus neuf.
      * --tools "" desactive tous les outils (c'est la valeur documentee pour
        "aucun"). Leurs definitions sont la plus grosse part de l'overhead
        restant, et le modele n'a rien a lire ni a ecrire : tout est dans stdin.

    Attention : --max-turns n'existe pas dans le CLI (verifie sur la 2.1.291) ; sans
    outils, la question ne se pose pas.
    """
    cmd = [binaire, "-p", DIRECTIVE,
           "--output-format", "json",
           "--system-prompt", systeme]
    if not outils:
        cmd += ["--tools", ""]
    if modele:
        cmd += ["--model", modele]
    if schema:
        cmd += ["--json-schema", json.dumps(SCHEMA)]
    return cmd + list(extra)


def extraire_json(texte):
    """
    Le premier objet JSON d'une reponse en texte libre.

    Sans schema impose, le modele peut encadrer sa reponse d'un bloc de code ou
    d'une phrase. On cherche donc le premier objet equilibre plutot que de
    supposer que toute la reponse est du JSON.
    """
    debut = texte.find("{")
    if debut < 0:
        raise ValueError("aucun objet JSON dans la reponse")
    profondeur, dans_chaine, echappe = 0, False, False
    for i, c in enumerate(texte[debut:], debut):
        if dans_chaine:
            if echappe:
                echappe = False
            elif c == "\\":
                echappe = True
            elif c == '"':
                dans_chaine = False
            continue
        if c == '"':
            dans_chaine = True
        elif c == "{":
            profondeur += 1
        elif c == "}":
            profondeur -= 1
            if profondeur == 0:
                return json.loads(texte[debut:i + 1])
    raise ValueError("objet JSON non termine")


class LimiteAtteinte(Exception):
    """Le quota de l'abonnement est epuise : rien a faire qu'attendre."""


MOTIFS_LIMITE = ("hit your session limit", "hit your usage limit",
                 "rate limit", "resets at", "resets ")


def repertoire_neutre():
    """
    Un repertoire vide ou faire tourner `claude`.

    Mesure a l'appui : lance depuis le depot, chaque invocation emporte le
    contexte du projet (CLAUDE.md, arborescence) -- 600 jetons de plus par appel
    et, plus grave, un correcteur qui a lu le depot avant de noter un appel. Le
    repertoire de travail du sous-processus ne doit donc rien contenir.
    """
    import tempfile
    global _NEUTRE
    try:
        return _NEUTRE
    except NameError:
        pass
    _NEUTRE = tempfile.mkdtemp(prefix="segmentation-")
    return _NEUTRE


def appeler_cli(cmd, message, timeout=600):
    """
    Une invocation. La transcription passe par stdin, pas par argv.

    Renvoie (note, usage, cout estime) ou leve LimiteAtteinte, qui est le seul
    echec qu'il ne sert a rien de reessayer.
    """
    import subprocess

    res = subprocess.run(cmd, input=message, capture_output=True,
                         text=True, timeout=timeout, cwd=repertoire_neutre())
    brut = (res.stdout or "") + (res.stderr or "")
    if any(m in brut.lower() for m in MOTIFS_LIMITE):
        raise LimiteAtteinte(brut.strip()[:300])
    if res.returncode != 0:
        raise RuntimeError(f"claude a renvoye {res.returncode} : {brut.strip()[:300]}")

    try:
        enveloppe = json.loads(res.stdout)
    except json.JSONDecodeError:
        raise RuntimeError(f"enveloppe illisible : {res.stdout[:200]}")

    if enveloppe.get("errors"):
        raise RuntimeError(f"erreurs CLI : {enveloppe['errors']}")

    # --json-schema, quand il est accepte, rend l'objet deja valide ;
    # sinon il faut l'extraire du texte.
    structure = enveloppe.get("structured_output")
    note = structure if isinstance(structure, dict) and structure \
        else extraire_json(enveloppe.get("result") or "")
    return note, enveloppe.get("usage") or {}, enveloppe.get("total_cost_usd")


def sonder_cli(systeme, gabarit, appel, modele, binaire, schema, extra,
               outils=False):
    """
    Un appel de controle, imprime en entier. A lancer avant toute passe.

    C'est ce qui transforme les options supposees du CLI en options verifiees :
    si l'une n'existe pas, on le voit ici, sur un appel, pas sur six mille.
    """
    cmd = construire_commande(systeme, modele, binaire, schema, extra, outils)
    affichable = [c if len(c) < 60 else c[:57] + "..." for c in cmd]
    print("Commande :")
    print("  " + " ".join(repr(c) if " " in c else c for c in affichable))
    message = rendre(gabarit, appel)
    print(f"\nstdin : {len(message):,} caracteres (appel {appel['call_id']})")
    t0 = time.time()
    note, usage, cout_cli = appeler_cli(cmd, message)
    print(f"\nReponse en {time.time()-t0:.0f} s :")
    print("  " + json.dumps(note, ensure_ascii=False))
    score, flag, ecart = recalculer(note)
    print(f"\nRecalcul : {flag} {score}/12" + ("  (le modele disait autre chose)" if ecart else ""))
    if usage:
        print(f"Jetons : {usage}")
    if cout_cli is not None:
        print(f"Cout affiche par le CLI : {cout_cli:.4f} $ "
              f"(estimation cote client ; sur abonnement, rien n'est facture)")
    return note


def noter_via_cli(conn, todo, systeme, gabarit, empreinte, modele, binaire,
                  schema, extra, workers=2, flush_every=20, outils=False):
    """
    La passe sur abonnement : un processus `claude` par appel, N en parallele.

    L'ecriture est faite par tranches : il n'y a pas de lot cote serveur a
    reprendre, donc une interruption ne doit pas perdre ce qui est deja note.
    L'anti-jointure fera le reste au prochain lancement.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    cmd = construire_commande(systeme, modele, binaire, schema, extra, outils)
    lignes, echecs, ecarts, usage_total = [], [], 0, {}
    cout_cli_total = 0.0
    limite = None
    t0 = time.time()

    def traiter(appel):
        note, usage, cout_cli = appeler_cli(cmd, rendre(gabarit, appel))
        return appel, note, usage, cout_cli

    with ThreadPoolExecutor(max_workers=workers) as pool:
        taches = {pool.submit(traiter, appel): appel
                  for _, appel in todo.iterrows()}
        for fini in as_completed(taches):
            appel = taches[fini]
            try:
                appel, note, usage, cout_cli = fini.result()
            except LimiteAtteinte as e:
                limite = str(e)
                for autre in taches:
                    autre.cancel()
                break
            except Exception as e:
                echecs.append((int(appel["call_id"]), type(e).__name__, str(e)[:120]))
                continue

            for cle, val in (usage or {}).items():
                if isinstance(val, (int, float)):
                    usage_total[cle] = usage_total.get(cle, 0) + val
            cout_cli_total += cout_cli or 0
            ecarts += bool(not note_vide(note) and recalculer(note)[2])
            ligne = ligne_resultat(appel["call_id"], appel["call_session"],
                                   note, modele, empreinte, None, "claude-code")
            lignes.append(ligne)

            fait = len(lignes) + len(echecs)
            if len(lignes) % flush_every == 0:
                ecrire(conn, lignes[-flush_every:])
                debit = fait / max(time.time() - t0, 1)
                reste = (len(todo) - fait) / max(debit, 1e-6)
                print(f"  {fait}/{len(todo)}  {len(lignes)} notes ecrites  "
                      f"{debit*60:.1f} appels/min  ~{reste/60:.0f} min restantes",
                      flush=True)

    # la derniere tranche incomplete
    deja = (len(lignes) // flush_every) * flush_every
    if lignes[deja:]:
        ecrire(conn, lignes[deja:])

    print(f"\n{len(lignes)} notes ecrites dans {TARGET_TABLE} "
          f"en {(time.time()-t0)/60:.1f} min.")
    if ecarts:
        print(f"  {ecarts} appels ou le score ou le flag du modele differait du "
              f"recalcul ({ecarts/max(len(lignes),1)*100:.0f} %) : c'est le "
              f"recalcul qui est stocke.")
    if echecs:
        print(f"  {len(echecs)} echecs :")
        for appel_id, genre, detail in echecs[:10]:
            print(f"    {appel_id} : {genre} {detail}")
        if len(echecs) > 10:
            print(f"    ... et {len(echecs) - 10} autres")
    # Le chiffre qui decide s'il est raisonnable d'etendre la passe : les
    # jetons REELLEMENT consommes par appel, overhead du CLI compris. Il n'est
    # pas predictible de l'exterieur -- chaque invocation emporte aussi le
    # prompt systeme de Claude Code -- donc on le mesure et on extrapole.
    if usage_total and lignes:
        par_appel = {k: v / len(lignes) for k, v in usage_total.items()}
        # Les trois postes d'entree, et il faut les trois : l'essentiel du
        # volume passe par cache_creation_input_tokens, puisque chaque appel
        # porte une transcription inedite. L'oublier divise le total par cinq.
        entree = (par_appel.get("input_tokens", 0)
                  + par_appel.get("cache_creation_input_tokens", 0)
                  + par_appel.get("cache_read_input_tokens", 0))
        sortie = par_appel.get("output_tokens", 0)
        lu = par_appel.get("cache_read_input_tokens", 0)
        print(f"  jetons par appel : {entree:,.0f} entree "
              f"(dont {lu:,.0f} lus en cache, {lu/max(entree,1)*100:.0f} %), "
              f"{sortie:,.0f} sortie")
        for n in (100, 1000, 5000):
            print(f"    pour {n:>5} appels : {(entree + sortie) * n / 1e6:>5.1f} M jetons")
        print(f"  detail : {usage_total}")
    if cout_cli_total:
        print(f"  equivalent API affiche par le CLI : {cout_cli_total:,.2f} $ "
              f"pour {len(lignes)} appels, soit {cout_cli_total/max(len(lignes),1):.3f} $/appel "
              f"(non facture sur abonnement)")
    if limite:
        print(f"\n/!\\ Quota d'abonnement atteint, passe interrompue :\n  {limite}")
        print("  Relancer la meme commande plus tard : les appels deja notes")
        print("  sont exclus par l'anti-jointure, rien n'est refait.")
    return lignes


# ---------------------------------------------------------------------------
# Rapports
# ---------------------------------------------------------------------------

def resumer(lignes):
    if not lignes:
        return
    df = pd.DataFrame(lignes)

    # L'accord d'abord : c'est lui qui dit combien d'appels on avait le droit
    # d'analyser. Les moyennes qui suivent ne portent que sur ceux-la.
    print("\nAccord d'enregistrement :")
    for valeur, nb in df["accord_enregistrement"].value_counts(dropna=False).items():
        print(f"  {str(valeur):<14} {nb:>5}  ({nb/len(df)*100:.0f} %)")

    notes = df[df["flag"] != FLAG_SANS_ACCORD]
    if notes.empty:
        print("\nAucun appel notable : pas un seul accord dans ce lot.")
        return
    print(f"\n--- la grille, sur les {len(notes)} appels avec accord ---")
    df = notes
    print("\nRepartition des flags :")
    for flag, nb in df["flag"].value_counts().items():
        print(f"  {flag:<12} {nb:>5}  ({nb/len(df)*100:.0f} %)")
    print(f"\nScore moyen : {df['score'].mean():.2f}/12")
    print("Moyenne par critere :")
    for n in NOTES:
        zeros = (df[n] == 0).mean() * 100
        print(f"  {n.upper()}  {df[n].mean():.2f}   (0 dans {zeros:.0f} % des appels)")
    print("\nOrigine de la PM :")
    for origine, nb in df["origine_pm"].value_counts().items():
        print(f"  {origine:<16} {nb:>5}  ({nb/len(df)*100:.0f} %)")


def comparer_aux_notes_manuelles(conn, lignes):
    """
    L'accord entre le modele et les 300 notes posees a la main.

    C'est la seule mesure qui dit si la grille automatisee vaut la grille
    d'origine. Sans elle, une passe complete produit 6 000 notes dont personne
    ne sait ce qu'elles valent.
    """
    ids = [l["id_appel"] for l in lignes]
    if not ids:
        return
    cur = conn.cursor()
    cur.execute(f"select * from {SEED_TABLE} where id_appel in "
                f"({','.join(str(i) for i in ids)})")
    colonnes = [d[0].lower() for d in cur.description]
    manuel = {int(r[colonnes.index("id_appel")]): dict(zip(colonnes, r))
              for r in cur.fetchall()}
    communs = [l for l in lignes if l["id_appel"] in manuel]
    if not communs:
        return

    print(f"\nAccord avec les notes manuelles, sur {len(communs)} appels communs :")
    print("  critere  identique  ecart<=1  ecart moyen")
    for n in NOTES + ["score"]:
        diffs = [abs(int(l[n]) - int(manuel[l["id_appel"]][n])) for l in communs]
        identique = sum(d == 0 for d in diffs) / len(diffs) * 100
        proche = sum(d <= 1 for d in diffs) / len(diffs) * 100
        print(f"  {n.upper():<8} {identique:>8.0f} % {proche:>8.0f} % "
              f"{sum(diffs)/len(diffs):>11.2f}")
    memes = sum(l["flag"] == manuel[l["id_appel"]]["flag"] for l in communs)
    print(f"  flag identique : {memes}/{len(communs)} ({memes/len(communs)*100:.0f} %)")

    plus_severe = sum(int(l["score"]) < int(manuel[l["id_appel"]]["score"]) for l in communs)
    plus_doux = sum(int(l["score"]) > int(manuel[l["id_appel"]]["score"]) for l in communs)
    print(f"  le modele note plus bas sur {plus_severe} appels, plus haut sur {plus_doux}.")


# ---------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--limit", type=int,
                    help="ne noter que les N appels les plus recents")
    ap.add_argument("--sessions", help="sessions a retenir, ex. 1,2,3 (defaut : toutes)")
    ap.add_argument("--min-duration", type=int, default=DUREE_MINI,
                    help=f"duree minimale en secondes (defaut {DUREE_MINI})")
    ap.add_argument("--min-densite", type=float, default=DENSITE_MINI,
                    help=f"caracteres de transcription par minute d'appel en "
                         f"dessous desquels la transcription est jugee ratee "
                         f"(defaut {DENSITE_MINI})")
    ap.add_argument("--engine", default="claude-code", choices=["claude-code", "api"],
                    help="claude-code : une invocation `claude -p` par appel, sur "
                         "l'abonnement (defaut). api : Batch API, facture aux jetons.")
    ap.add_argument("--model", default=MODELE_DEFAUT, help=f"defaut {MODELE_DEFAUT}")
    ap.add_argument("--claude-bin", default="claude",
                    help="chemin du CLI claude (defaut : celui du PATH)")
    ap.add_argument("--workers", type=int, default=2,
                    help="invocations `claude` en parallele (defaut 2)")
    ap.add_argument("--flush-every", type=int, default=20,
                    help="ecrire dans Snowflake toutes les N notes (defaut 20)")
    ap.add_argument("--json-schema", action="store_true",
                    help="passer le schema au CLI (--json-schema) plutot que "
                         "d'extraire le JSON du texte ; a tester avec --probe")
    ap.add_argument("--avec-outils", action="store_true",
                    help="laisser les outils actifs (par defaut --tools '' les "
                         "desactive : moins de jetons, aucune utilite ici)")
    ap.add_argument("--probe", action="store_true",
                    help="un seul appel de controle, commande et reponse imprimees")
    ap.add_argument("--claude-flag", action="append", default=[], metavar="FLAG",
                    help="option supplementaire passee telle quelle a `claude` "
                         "(repetable), ex. --claude-flag=--tools --claude-flag=''")
    ap.add_argument("--effort", default="high",
                    choices=["low", "medium", "high", "xhigh", "max"],
                    help="profondeur de reflexion, premier levier de cout (defaut high)")
    ap.add_argument("--tous-les-appels", action="store_true",
                    help="noter tous les appels reels, pas seulement le plus long "
                         "de chaque session (par defaut un seul, celui que "
                         "aircall_analysis retient)")
    ap.add_argument("--random", action="store_true",
                    help="tirer les appels au hasard au lieu des plus recents")
    ap.add_argument("--seed", type=int,
                    help="graine du tirage, pour qu'il soit reproductible")
    ap.add_argument("--only-notes", action="store_true",
                    help="calibrage : re-noter des appels deja notes a la main")
    ap.add_argument("--estimate", action="store_true",
                    help="compter les jetons et chiffrer, sans rien depenser")
    ap.add_argument("--sync", action="store_true",
                    help="appels immediats hors Batch API (plein tarif, pour quelques appels)")
    ap.add_argument("--no-wait", action="store_true",
                    help="soumettre les lots et rendre la main (--collect plus tard)")
    ap.add_argument("--collect", nargs="?", const="*", metavar="BATCH_ID",
                    help="recuperer un lot deja soumis, ou tous ceux en attente")
    ap.add_argument("--reflag", action="store_true",
                    help="recalculer flag et score de toutes les lignes sous la "
                         "regle courante, sans renoter")
    ap.add_argument("--import-seed", action="store_true",
                    help="verser les 300 notes manuelles dans la table cible")
    ap.add_argument("--force", action="store_true",
                    help="renoter meme les appels deja presents dans la table")
    args = ap.parse_args()

    systeme, gabarit, empreinte = charger_prompt()
    conn = sf_connect()
    creer_table(conn)

    if args.reflag:
        reetiqueter(conn)
        conn.close()
        return

    if args.import_seed:
        importer_seed(conn)
        if not (args.limit or args.collect):
            conn.close()
            return

    client = None
    if args.engine == "api":
        import anthropic
        client = anthropic.Anthropic()
    elif args.collect:
        sys.exit("--collect ne concerne que le moteur api (lots deja soumis).")

    # --- recuperation d'un lot deja paye -----------------------------------
    if args.collect:
        lots = [l for l in lire_etat() if not l.get("recupere_le")]
        if args.collect != "*":
            lots = [l for l in lots if l["batch_id"] == args.collect]
        if not lots:
            print("Aucun lot en attente dans " + STATE_PATH)
            conn.close()
            return
        toutes = []
        for journal in lots:
            attendre(client, journal["batch_id"])
            toutes += collecter(client, conn, journal["batch_id"], journal)
        resumer(toutes)
        comparer_aux_notes_manuelles(conn, toutes)
        conn.close()
        return

    # --- selection du travail ----------------------------------------------
    sessions = [int(s) for s in args.sessions.split(",")] if args.sessions else None
    todo, ecartes = fetch_todo(conn, limit=args.limit, sessions=sessions,
                               duree_mini=args.min_duration,
                               densite_mini=args.min_densite,
                               only_notes=args.only_notes, force=args.force,
                               alea=args.random, graine=args.seed,
                               un_par_session=not args.tous_les_appels)
    if todo.empty:
        print("Rien a noter : tous les appels notables ont deja une note.")
        conn.close()
        return

    print(f"{len(todo)} appels a noter, {todo['nb_car'].sum()/1e6:.1f} Mo de "
          f"transcription, {todo['nb_car'].mean():,.0f} caracteres en moyenne.")
    if ecartes:
        print(f"{ecartes} appels ecartes : moins de {args.min_densite} caracteres "
              f"par minute, transcription inexploitable.")
    moteur = ("abonnement (claude -p)" if args.engine == "claude-code"
              else f"API, effort {args.effort}")
    print(f"Grille {empreinte} · modele {args.model} · {moteur}")

    if args.estimate:
        if args.engine == "api":
            estimer(client, todo, systeme, gabarit, args.model, args.effort)
        else:
            estimer_abonnement(todo, systeme, args.workers)
        conn.close()
        return

    if args.engine == "claude-code":
        if args.probe:
            sonder_cli(systeme, gabarit, todo.iloc[0], args.model,
                       args.claude_bin, args.json_schema, args.claude_flag,
                       args.avec_outils)
            conn.close()
            return
        lignes = noter_via_cli(conn, todo, systeme, gabarit, empreinte,
                               args.model, args.claude_bin, args.json_schema,
                               args.claude_flag, workers=args.workers,
                               flush_every=args.flush_every,
                               outils=args.avec_outils)
        resumer(lignes)
        comparer_aux_notes_manuelles(conn, lignes)
        print("\nRelancer ensuite :")
        print("  dbt run -s aircall_analysis --profiles-dir dbt")
        conn.close()
        return

    if args.sync:
        lignes = noter_en_direct(client, conn, todo, systeme, gabarit,
                                 args.model, args.effort, empreinte)
    else:
        print(f"\nSoumission par lots de {TAILLE_LOT} (Batch API, -50 %)...")
        ids = soumettre(client, todo, systeme, gabarit,
                        args.model, args.effort, empreinte)
        if args.no_wait:
            print(f"\n{len(ids)} lots soumis. Les recuperer plus tard :")
            print("  python admin/segment_appels_llm.py --collect")
            conn.close()
            return
        lignes = []
        journaux = {l["batch_id"]: l for l in lire_etat()}
        for batch_id in ids:
            attendre(client, batch_id)
            lignes += collecter(client, conn, batch_id, journaux[batch_id])

    resumer(lignes)
    comparer_aux_notes_manuelles(conn, lignes)

    print("\nRelancer ensuite :")
    print("  dbt run -s aircall_analysis --profiles-dir dbt")
    conn.close()


if __name__ == "__main__":
    main()
