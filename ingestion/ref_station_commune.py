"""Rattachement des stations à une commune : com_arm_code du flux quotidien (jeu prix-carburants-quotidien), table ref_station_commune.

Rejeu, dans le conteneur airflow : python -m ingestion.ref_station_commune --telecharger   (ou make charger-station-commune)
Chaîne : téléchargement de l'export JSON (id, com_arm_code, prix_maj), lecture (une ligne par station), validation GX, puis
remplacement complet de la table en une transaction. Une station avec plusieurs codes différents arrête le chargement.
fichier_date : jour UTC du dernier prix_maj du fichier (le quotidien est un état à J-1).
Licence : « Licence Ouverte (Etalab) » (métadonnées du jeu, sans numéro de version).
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
from pathlib import Path

import pandas as pd
from psycopg2.extras import execute_values

from ingestion.referentiels import ErreurReferentiel
from quality.gx.ref_station_commune import valider_station_commune

URL_QUOTIDIEN = "https://data.economie.gouv.fr/api/explore/v2.1/catalog/datasets/prix-carburants-quotidien/exports/json"
PARAMETRES = {"select": "id,com_arm_code,prix_maj", "limit": -1}
NOM_FICHIER = "quotidien_{horodatage}.json"

DDL = """
CREATE TABLE IF NOT EXISTS ref_station_commune (
    station_id BIGINT PRIMARY KEY, com_arm_code TEXT, fichier_date DATE NOT NULL,
    charge_ts TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""


def _obtenir_http(url: str, params: dict) -> bytes:
    import requests

    reponse = requests.get(url, params=params, headers={"User-Agent": "observatoire-carburant"}, timeout=(10, 300))
    reponse.raise_for_status()
    return reponse.content


def telecharger_quotidien(dossier: Path, obtenir=_obtenir_http) -> tuple[Path, int, str]:
    """Télécharge l'export du quotidien dans `dossier`. Rend (chemin, octets, empreinte sha256)."""
    dossier = Path(dossier)
    dossier.mkdir(parents=True, exist_ok=True)
    contenu = obtenir(URL_QUOTIDIEN, dict(PARAMETRES))
    chemin = dossier / NOM_FICHIER.format(horodatage=dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ"))
    chemin.write_bytes(contenu)
    return chemin, len(contenu), hashlib.sha256(contenu).hexdigest()


def _vide(valeur) -> bool:
    return valeur is None or str(valeur).strip() == ""


def lire_quotidien(chemin: Path) -> tuple[pd.DataFrame, dt.date]:
    """Une ligne par station (station_id, com_arm_code), triée ; code vide à None. Rend aussi la date du fichier."""
    with open(chemin, encoding="utf-8") as f:
        lignes = json.load(f)
    if not isinstance(lignes, list):
        raise ErreurReferentiel(f"{Path(chemin).name} : une liste de lignes est attendue")
    codes: dict[int, set[str]] = {}
    dernier_maj = None
    for ligne in lignes:
        if ligne.get("id") is None:
            continue
        station = int(ligne["id"])
        ensemble = codes.setdefault(station, set())
        if not _vide(ligne.get("com_arm_code")):
            ensemble.add(str(ligne["com_arm_code"]).strip())
        if not _vide(ligne.get("prix_maj")):
            maj = dt.datetime.fromisoformat(ligne["prix_maj"]).astimezone(dt.timezone.utc)
            dernier_maj = maj if dernier_maj is None or maj > dernier_maj else dernier_maj
    multiples = sorted(s for s, c in codes.items() if len(c) > 1)
    if multiples:
        raise ErreurReferentiel(f"plusieurs com_arm_code pour {len(multiples)} station(s) : {multiples[:10]}")
    if dernier_maj is None:
        raise ErreurReferentiel(f"{Path(chemin).name} : aucun prix_maj, date du fichier inconnue")
    df = pd.DataFrame(
        [(s, next(iter(c)) if c else None) for s, c in sorted(codes.items())],
        columns=["station_id", "com_arm_code"],
    ).astype({"com_arm_code": object})
    return df, dernier_maj.date()


def charger_station_commune(conn, df: pd.DataFrame, fichier_date: dt.date, validateur=valider_station_commune) -> int:
    """Valide (GX) puis remplace tout le contenu de ref_station_commune, en une transaction."""
    verdict = validateur(df)
    if not verdict["success"]:
        echecs = ", ".join(sorted({e["expectation"] for e in verdict["echecs"]}))
        raise ErreurReferentiel(f"validation GX en échec pour ref_station_commune : {echecs}")
    lignes = [(int(s), None if _vide(c) else c, fichier_date) for s, c in df.itertuples(index=False, name=None)]
    # Une seule transaction (DDL, DELETE, INSERT, puis un seul commit) : un échec à l'insertion laisse la table précédente.
    try:
        with conn.cursor() as cur:
            cur.execute(DDL)
            cur.execute("DELETE FROM ref_station_commune")
            execute_values(cur, "INSERT INTO ref_station_commune (station_id, com_arm_code, fichier_date) VALUES %s", lignes)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return len(lignes)


def main(argv: list[str] | None = None) -> int:
    from ingestion.db import connecter

    parseur = argparse.ArgumentParser(description="Chargement de ref_station_commune (com_arm_code du flux quotidien).")
    parseur.add_argument("--telecharger", action="store_true", help="télécharge d'abord l'export du quotidien")
    parseur.add_argument("--fichier", help="fichier JSON déjà téléchargé (sinon : le plus récent du dossier)")
    arguments = parseur.parse_args(argv)
    dossier = Path(os.environ.get("REFERENTIELS_DIR", "/opt/airflow/data/referentiels")) / "station_commune"
    if arguments.telecharger:
        chemin, octets, empreinte = telecharger_quotidien(dossier)
        print(f"téléchargé {chemin.name} : {octets} octets, sha256 {empreinte[:16]}…")
    elif arguments.fichier:
        chemin = Path(arguments.fichier)
    else:
        fichiers = sorted(dossier.glob("quotidien_*.json"))
        if not fichiers:
            raise ErreurReferentiel(f"aucun fichier dans {dossier} : utiliser --telecharger")
        chemin = fichiers[-1]
    print(f"fichier choisi : {chemin}")
    df, fichier_date = lire_quotidien(chemin)
    print(f"fichier_date : {fichier_date} ; {len(df)} stations, {int(df['com_arm_code'].notna().sum())} avec code")
    conn = connecter()
    try:
        n = charger_station_commune(conn, df, fichier_date)
    finally:
        conn.close()
    print(f"ref_station_commune : {n} lignes")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
