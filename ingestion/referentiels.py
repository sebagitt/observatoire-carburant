"""Référentiels géographiques (COG de l'INSEE) : téléchargement, validation GX, chargement dans la base collecte.

Rejeu, dans le conteneur airflow : python -m ingestion.referentiels cog --telecharger   (ou make charger-cog)
                                  python -m ingestion.referentiels filosofi --telecharger   (ou make charger-filosofi)
Le chargement est idempotent : il remplace en une transaction les lignes du millésime demandé, sans toucher aux autres.
Licence (COG et Filosofi) : Licence Ouverte 2.0 (Etalab) selon les mentions légales de l'INSEE ; mentionner « Source : Insee » et la date de mise à jour.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import io
import os
import zipfile
from pathlib import Path

import pandas as pd
from psycopg2.extras import execute_values

from quality.gx.ref_cog_commune import valider_cog_commune
from quality.gx.ref_filosofi_commune import valider_filosofi_commune

PAGES_COG = {2026: 8740222}  # identifiant de la page « Code officiel géographique » de chaque millésime
PAGES_FILOSOFI = {2023: 8984752}  # identifiant de la page « Revenus et pauvreté au niveau local » de chaque millésime
NOM_ZIP_FILOSOFI = "FILOSOFI_CC_csv.zip"
COLONNES_FILOSOFI_DONNEES = ["GEO", "GEO_OBJECT", "FILOSOFI_MEASURE", "UNIT_MULT", "UNIT_MEASURE", "CONF_STATUS", "OBS_STATUS", "TIME_PERIOD", "OBS_VALUE"]
COLONNES_FILOSOFI = ["GEO_OBJECT", "CODGEO", "MED_SL", "MED_SL_STATUT", "MED_SL_CONF", "PR_MD60", "PR_MD60_STATUT", "PR_MD60_CONF"]
NIVEAUX_FILOSOFI = ("COM", "ARM")  # commune et arrondissement municipal (Paris, Lyon et Marseille figurent aux deux niveaux)
MESURES_FILOSOFI = ("MED_SL", "PR_MD60")  # niveau de vie médian (euros par an) et taux de pauvreté à 60 % (pourcentage)
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


DDL_FILOSOFI = """
CREATE TABLE IF NOT EXISTS ref_filosofi_commune (
    millesime INTEGER NOT NULL, geo_object TEXT NOT NULL, codgeo TEXT NOT NULL,
    med_sl NUMERIC, med_sl_statut TEXT, med_sl_conf TEXT,
    pr_md60 NUMERIC, pr_md60_statut TEXT, pr_md60_conf TEXT,
    charge_ts TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (millesime, geo_object, codgeo)
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


def lire_filosofi(source: Path, millesime: int = 2023) -> pd.DataFrame:
    """Lit le fichier de données de Filosofi (format long, point-virgule), directement dans le zip, en flux.

    Ne garde que les niveaux COM et ARM et les mesures MED_SL et PR_MD60, mises sur une ligne par zone avec leurs statuts
    (OBS_STATUS : A normale, O valeur manquante ; CONF_STATUS : F libre, C confidentielle). Une valeur vide devient None.
    """
    source = Path(source)
    nom_donnees = f"DS_FILOSOFI_CC_{millesime}_data.csv"
    if not zipfile.is_zipfile(source):
        with open(source, encoding="utf-8-sig", newline="") as f:
            return _lire_flux_filosofi(f, millesime, source.name)
    with zipfile.ZipFile(source) as z:
        if nom_donnees not in z.namelist():
            raise ErreurReferentiel(f"fichier de données {nom_donnees} absent de {source.name}")
        with z.open(nom_donnees) as brut:
            return _lire_flux_filosofi(io.TextIOWrapper(brut, encoding="utf-8-sig", newline=""), millesime, nom_donnees)


def _lire_flux_filosofi(texte, millesime: int, nom: str) -> pd.DataFrame:
    lecteur = csv.reader(texte, delimiter=";")
    entete = next(lecteur, None)
    if entete != COLONNES_FILOSOFI_DONNEES:
        raise ErreurReferentiel(f"colonnes inattendues dans {nom}")
    rang = {colonne: i for i, colonne in enumerate(entete)}
    zones: dict[tuple[str, str], dict[str, tuple]] = {}
    for numero, ligne in enumerate(lecteur, start=2):
        if not ligne:
            continue
        if len(ligne) != len(entete):
            raise ErreurReferentiel(f"ligne {numero} de {nom} : {len(ligne)} colonnes au lieu de {len(entete)}")
        niveau, mesure = ligne[rang["GEO_OBJECT"]], ligne[rang["FILOSOFI_MEASURE"]]
        if niveau not in NIVEAUX_FILOSOFI or mesure not in MESURES_FILOSOFI:
            continue
        if ligne[rang["TIME_PERIOD"]] != str(millesime):
            raise ErreurReferentiel(f"TIME_PERIOD inattendu ligne {numero} de {nom}")
        brute = ligne[rang["OBS_VALUE"]]
        try:
            valeur = float(brute) if brute != "" else None
        except ValueError:
            raise ErreurReferentiel(f"OBS_VALUE non numérique ligne {numero} de {nom}") from None
        zones.setdefault((niveau, ligne[rang["GEO"]]), {})[mesure] = (valeur, ligne[rang["OBS_STATUS"]], ligne[rang["CONF_STATUS"]])
    vide = (None, None, None)
    lignes = [
        [niveau, geo, *mesures.get("MED_SL", vide), *mesures.get("PR_MD60", vide)]
        for (niveau, geo), mesures in sorted(zones.items())
    ]
    return pd.DataFrame(lignes, columns=COLONNES_FILOSOFI, dtype=object)


def telecharger_filosofi(dossier: Path, millesime: int = 2023, obtenir=_obtenir_http) -> dict[str, tuple[int, str]]:
    """Télécharge le zip Filosofi (environ 5 Mo) dans `dossier`. Rend {nom: (octets, empreinte sha256)}."""
    if millesime not in PAGES_FILOSOFI:
        raise ErreurReferentiel(f"millésime {millesime} : page INSEE inconnue")
    dossier = Path(dossier)
    dossier.mkdir(parents=True, exist_ok=True)
    contenu = obtenir(URL_COG.format(page=PAGES_FILOSOFI[millesime], nom=NOM_ZIP_FILOSOFI))
    (dossier / NOM_ZIP_FILOSOFI).write_bytes(contenu)
    return {NOM_ZIP_FILOSOFI: (len(contenu), hashlib.sha256(contenu).hexdigest())}


def charger_filosofi(conn, source: Path, millesime: int = 2023, validateur=valider_filosofi_commune) -> dict[str, int]:
    """Valide (GX) puis remplace, en une transaction, les lignes du millésime dans ref_filosofi_commune."""
    df = lire_filosofi(source, millesime)
    numerique = df.assign(MED_SL=pd.to_numeric(df["MED_SL"]), PR_MD60=pd.to_numeric(df["PR_MD60"]))
    verdict = validateur(numerique)
    if not verdict["success"]:
        echecs = ", ".join(sorted({e["expectation"] for e in verdict["echecs"]}))
        raise ErreurReferentiel(f"validation GX en échec pour Filosofi {millesime} : {echecs}")
    try:
        with conn.cursor() as cur:
            cur.execute(DDL_FILOSOFI)
            n = _inserer(cur, "ref_filosofi_commune", millesime, df)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return {"ref_filosofi_commune": n}


def main(argv: list[str] | None = None) -> int:
    from ingestion.db import connecter

    parseur = argparse.ArgumentParser(description="Chargement des référentiels dans la base collecte.")
    parseur.add_argument("referentiel", choices=["cog", "filosofi"])
    parseur.add_argument("--millesime", type=int, help="défaut : 2026 pour le COG, 2023 pour Filosofi")
    parseur.add_argument("--dossier", help="dossier des fichiers (défaut : $REFERENTIELS_DIR/<référentiel>/<millésime>)")
    parseur.add_argument("--telecharger", action="store_true", help="télécharge d'abord les fichiers depuis l'INSEE")
    arguments = parseur.parse_args(argv)
    millesime = arguments.millesime or (2026 if arguments.referentiel == "cog" else 2023)
    racine = Path(os.environ.get("REFERENTIELS_DIR", "/opt/airflow/data/referentiels"))
    dossier = Path(arguments.dossier) if arguments.dossier else racine / arguments.referentiel / str(millesime)
    if arguments.telecharger:
        telechargement = telecharger_cog if arguments.referentiel == "cog" else telecharger_filosofi
        for nom, (octets, empreinte) in telechargement(dossier, millesime).items():
            print(f"téléchargé {nom} : {octets} octets, sha256 {empreinte[:16]}…")
    conn = connecter()
    try:
        if arguments.referentiel == "cog":
            comptes = charger_cog(conn, dossier, millesime)
        else:
            comptes = charger_filosofi(conn, dossier / NOM_ZIP_FILOSOFI, millesime)
    finally:
        conn.close()
    for table, n in comptes.items():
        print(f"{table} : {n} lignes (millésime {millesime})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
