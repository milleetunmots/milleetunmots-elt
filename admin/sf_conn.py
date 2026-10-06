"""
Connexion Snowflake pour les scripts admin, lue dans dbt/profiles.yml.

Mutualise ce qui etait jusqu'ici recopie dans chaque script : la lecture du
profil dbt et la conversion de la cle privee PEM en DER. Les deux scripts qui
ecrivent dans l'entrepot -- anonymize_aircall_ner.py et segment_appels_llm.py --
passent par ici, pour que l'authentification n'ait qu'un seul endroit ou
deriver.
"""

import os
import sys

import yaml

BASE = os.path.dirname(os.path.abspath(__file__))
PROFILES_PATH = os.path.join(BASE, "..", "dbt", "profiles.yml")


def load_sf_config():
    with open(PROFILES_PATH) as f:
        profiles = yaml.safe_load(f)
    profile = next(iter(profiles.values()))
    return profile["outputs"][profile.get("target", "prod")]


def sf_connect(config=None):
    """
    Connexion suivant ce que declare dbt/profiles.yml.

    Le profil est passe en authentification par paire de cles ; le mot de passe
    n'y figure plus. C'est la raison pour laquelle admin/anonymize_transcripts.py,
    ecrit avant la bascule, echoue desormais sur un KeyError 'password'.
    """
    import snowflake.connector

    config = config or load_sf_config()

    kwargs = dict(
        account=config["account"],
        user=config["user"],
        role=config.get("role"),
        warehouse=config.get("warehouse"),
        database=config.get("database"),
        schema=config.get("schema"),
    )

    if config.get("private_key_path"):
        from cryptography.hazmat.backends import default_backend
        from cryptography.hazmat.primitives import serialization

        chemin = os.path.expanduser(config["private_key_path"])
        passphrase = config.get("private_key_passphrase")
        with open(chemin, "rb") as f:
            cle = serialization.load_pem_private_key(
                f.read(),
                password=passphrase.encode() if passphrase else None,
                backend=default_backend(),
            )
        # le connecteur attend la cle au format DER, pas le PEM du fichier
        kwargs["private_key"] = cle.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )
    elif config.get("password"):
        kwargs["password"] = config["password"]
    else:
        sys.exit("Ni private_key_path ni password dans dbt/profiles.yml.")

    return snowflake.connector.connect(**kwargs)
