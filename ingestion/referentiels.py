"""Référentiels géographiques (COG de l'INSEE) : téléchargement, validation GX, chargement dans la base collecte.

Rejeu, dans le conteneur airflow : python -m ingestion.referentiels cog --telecharger   (ou make charger-cog)
Le chargement est idempotent : il remplace en une transaction les lignes du millésime demandé, sans toucher aux autres.
Licence : Licence Ouverte 2.0 (Etalab) selon les mentions légales de l'INSEE ; mentionner « Source : Insee » et la date de mise à jour.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import os
from pathlib import Path

import pandas as pd
from psycopg2.extras import execute_values

from quality.gx.ref_cog_commune import valider_cog_commune

PAGES_COG = {2026: 8740222}  # identifiant de la page « Code officiel géographique » de chaque millésime
URL_COG = "https://www.insee.fr/fr/statistiques/fichier/{page}/{nom}"
COLONNES_COMMUNE = ["TYPECOM", "COM", "REG", "DEP", "CTCD", "ARR", "TNCC", "NCC", "NCCENR", "LIBELLE", "CAN", "COMPARENT"]
COLONNES_DEPARTEMENT = ["DEP", "REG", "CHEFLIEU", "TNCC", "NCC", "NCCENR", "LIBELLE"]
COLONNES_REGION = ["REG", "CHEFLIEU", "TNCC", "NCC", "NCCENR", "LIBELLE"]

DDL_COG = """
CREATE TABLE IF NOT EXISTS ref_cog_commune (
    millesime INTEGER NOT NULL, typecom TEXT NOT NULL, com TEXT NOT NULL, reg TEXT, dep TEXT, ctcd TEXT, arr TEXT,
    tncc TEXT, ncc TEXT, nccenr TEXT, libelle TEXT NOT NULL, can TEXT, comparent TEXT,
    charge_ts TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (millesime, typecom, com)
);
CREATE TABLE IF NOT EXISTS ref_cog_departement (
    millesime INTEGER NOT NULL, dep TEXT NOT NULL, reg TEXT, cheflieu TEXT, tncc TEXT, ncc TEXT, nccenr TEXT,
    libelle TEXT NOT NULL, charge_ts TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (millesime, dep)
);
CREATE TABLE IF NOT EXISTS ref_cog_region (
    millesime INTEGER NOT NULL, reg TEXT NOT NULL, cheflieu TEXT, tncc TEXT, ncc TEXT, nccenr TEXT,
    libelle TEXT NOT NULL, charge_ts TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (millesime, reg)
);
"""


class ErreurReferentiel(Exception):
    """Fichier de référentiel inattendu ou lot refusé par la validation."""


def lire_csv(chemin: Path, colonnes: list[str]) -> pd.DataFrame:
    """Lit un CSV du COG (virgule, tout entre guillemets, UTF-8) en texte : zéros de tête et 2A/2B conservés, vide = None."""
    chemin = Path(chemin)
    with open(chemin, encoding="utf-8-sig", newline="") as f:
        lecteur = csv.reader(f)
        entete = next(lecteur, None)
        if entete != list(colonnes):
            raise ErreurReferentiel(f"colonnes inattendues dans {chemin.name}")
        lignes = []
        for numero, ligne in enumerate(lecteur, start=2):
            if not ligne:
                continue
            if len(ligne) != len(colonnes):
                raise ErreurReferentiel(f"ligne {numero} de {chemin.name} : {len(ligne)} colonnes au lieu de {len(colonnes)}")
            lignes.append([valeur if valeur != "" else None for valeur in ligne])
    return pd.DataFrame(lignes, columns=list(colonnes), dtype=object)


def _noms_cog(millesime: int) -> dict[str, str]:
    return {"commune": f"v_commune_{millesime}.csv", "departement": f"v_departement_{millesime}.csv", "region": f"v_region_{millesime}.csv"}


def _obtenir_http(url: str) -> bytes:
    import requests

    reponse = requests.get(url, headers={"User-Agent": "observatoire-carburant-etude/0.1"}, timeout=(10, 120))
    reponse.raise_for_status()
    return reponse.content


def telecharger_cog(dossier: Path, millesime: int = 2026, obtenir=_obtenir_http) -> dict[str, tuple[int, str]]:
    """Télécharge les trois fichiers du COG dans `dossier`. Rend, par fichier, (octets, empreinte sha256)."""
    if millesime not in PAGES_COG:
        raise ErreurReferentiel(f"millésime {millesime} : page INSEE inconnue")
    dossier = Path(dossier)
    dossier.mkdir(parents=True, exist_ok=True)
    resultat = {}
    for nom in _noms_cog(millesime).values():
        contenu = obtenir(URL_COG.format(page=PAGES_COG[millesime], nom=nom))
        (dossier / nom).write_bytes(contenu)
        resultat[nom] = (len(contenu), hashlib.sha256(contenu).hexdigest())
    return resultat


def _inserer(cur, table: str, millesime: int, df: pd.DataFrame) -> int:
    colonnes = ", ".join(c.lower() for c in df.columns)
    cur.execute(f"DELETE FROM {table} WHERE millesime = %s", (millesime,))
    execute_values(
        cur,
        f"INSERT INTO {table} (millesime, {colonnes}) VALUES %s",
        [(millesime, *ligne) for ligne in df.itertuples(index=False, name=None)],
    )
    return len(df)


def charger_cog(conn, dossier: Path, millesime: int = 2026, validateur=valider_cog_commune) -> dict[str, int]:
    """Valide (GX) puis remplace, en une seule transaction, les lignes du millésime dans les trois tables ref_cog_*."""
    noms = _noms_cog(millesime)
    dossier = Path(dossier)
    communes = lire_csv(dossier / noms["commune"], COLONNES_COMMUNE)
    departements = lire_csv(dossier / noms["departement"], COLONNES_DEPARTEMENT)
    regions = lire_csv(dossier / noms["region"], COLONNES_REGION)
    verdict = validateur(communes)
    if not verdict["success"]:
        echecs = ", ".join(sorted({e["expectation"] for e in verdict["echecs"]}))
        raise ErreurReferentiel(f"validation GX en échec pour {noms['commune']} : {echecs}")
    try:
        with conn.cursor() as cur:
            cur.execute(DDL_COG)
            comptes = {
                "ref_cog_commune": _inserer(cur, "ref_cog_commune", millesime, communes),
                "ref_cog_departement": _inserer(cur, "ref_cog_departement", millesime, departements),
                "ref_cog_region": _inserer(cur, "ref_cog_region", millesime, regions),
            }
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return comptes


def main(argv: list[str] | None = None) -> int:
    from ingestion.db import connecter

    parseur = argparse.ArgumentParser(description="Chargement des référentiels dans la base collecte.")
    parseur.add_argument("referentiel", choices=["cog"])
    parseur.add_argument("--millesime", type=int, default=2026)
    parseur.add_argument("--dossier", help="dossier des CSV (défaut : $REFERENTIELS_DIR/cog/<millésime>)")
    parseur.add_argument("--telecharger", action="store_true", help="télécharge d'abord les fichiers depuis l'INSEE")
    arguments = parseur.parse_args(argv)
    racine = Path(os.environ.get("REFERENTIELS_DIR", "/opt/airflow/data/referentiels"))
    dossier = Path(arguments.dossier) if arguments.dossier else racine / "cog" / str(arguments.millesime)
    if arguments.telecharger:
        for nom, (octets, empreinte) in telecharger_cog(dossier, arguments.millesime).items():
            print(f"téléchargé {nom} : {octets} octets, sha256 {empreinte[:16]}…")
    conn = connecter()
    try:
        comptes = charger_cog(conn, dossier, arguments.millesime)
    finally:
        conn.close()
    for table, n in comptes.items():
        print(f"{table} : {n} lignes (millésime {arguments.millesime})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
