"""Extraction et mise à plat du flux instantané des prix carburants (v2)."""
from __future__ import annotations

import hashlib
import json
from zoneinfo import ZoneInfo

import pandas as pd
import requests

# À aligner sur la fiche-source du J1 si l'URL ou les noms de champs diffèrent.
FLUX_URL = (
    "https://data.economie.gouv.fr/api/explore/v2.1/catalog/datasets/"
    "prix-des-carburants-en-france-flux-instantane-v2/exports/json"
)
CARBURANTS = ("gazole", "sp95", "sp98", "e10", "e85", "gplc")
COLONNES = ["station_id", "carburant", "maj", "prix"]
PARIS = ZoneInfo("Europe/Paris")


def telecharger_flux(url: str = FLUX_URL, timeout: tuple[int, int] = (10, 120)) -> list[dict]:
    reponse = requests.get(url, timeout=timeout)
    reponse.raise_for_status()
    stations = reponse.json()
    if not isinstance(stations, list):
        raise ValueError(f"Réponse inattendue du flux : {type(stations).__name__}")
    return stations


def _vers_utc(valeur) -> pd.Timestamp:
    """Lit une date du flux comme heure murale de Paris, puis la convertit en UTC.

    La source étiquette ses dates en UTC alors qu'elles sont en heure locale (constaté le
    2026-10-23 : maj la plus récente en avance de 2 h sur l'heure UTC réelle). On ignore donc
    l'offset annoncé. Limite : l'heure répétée lors du passage à l'heure d'hiver est
    ambiguë, on retient la première occurrence (heure d'été).
    """
    if valeur in (None, ""):
        return pd.NaT
    ts = pd.Timestamp(valeur)
    if ts.tzinfo is not None:
        ts = ts.tz_localize(None)  # garde l'heure murale, retire l'étiquette
    return ts.tz_localize(PARIS, ambiguous=True, nonexistent="shift_forward").tz_convert("UTC")


def aplatir_prix(stations: list[dict]) -> pd.DataFrame:
    """Une ligne par (station, carburant) ayant un prix : station_id, carburant, maj (UTC), prix."""
    lignes = []
    for s in stations:
        if s.get("id") is None:
            continue
        for carburant in CARBURANTS:
            prix = s.get(f"{carburant}_prix")
            if prix is None:
                continue
            lignes.append((int(s["id"]), carburant, _vers_utc(s.get(f"{carburant}_maj")), float(prix)))
    df = pd.DataFrame(lignes, columns=COLONNES)
    if df.empty:
        return df
    df["maj"] = pd.to_datetime(df["maj"], utc=True)
    return df


COLONNES_ETAT = ["station_id", "etat_hash", "etat"]


def _est_volatil(cle: str) -> bool:
    """Clés qui changent avec les prix (déjà portés par prix_flux) ou identifient la station."""
    return cle in ("id", "prix") or cle.endswith(("_prix", "_maj"))


def _normaliser(valeur):
    """Forme canonique pour le hash : l'ordre des listes ne compte pas."""
    if isinstance(valeur, dict):
        return {k: _normaliser(v) for k, v in valeur.items()}
    if isinstance(valeur, list):
        return sorted((_normaliser(v) for v in valeur), key=lambda v: json.dumps(v, sort_keys=True))
    return valeur


def construire_etats(stations: list[dict]) -> pd.DataFrame:
    """Une ligne par station : station_id, etat_hash, etat (JSON des attributs hors prix)."""
    lignes = []
    for s in stations:
        if s.get("id") is None:
            continue
        etat = {k: v for k, v in s.items() if not _est_volatil(k)}
        canonique = json.dumps(_normaliser(etat), sort_keys=True, ensure_ascii=False)
        lignes.append(
            (
                int(s["id"]),
                hashlib.sha256(canonique.encode("utf-8")).hexdigest(),
                json.dumps(etat, sort_keys=True, ensure_ascii=False),
            )
        )
    return pd.DataFrame(lignes, columns=COLONNES_ETAT)
