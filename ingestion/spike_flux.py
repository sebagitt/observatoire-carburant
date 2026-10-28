"""Spike J1 : inspecter le flux prix carburants et mesurer le taux de changement.

Usage :
    python -m ingestion.spike_flux snapshot
    python -m ingestion.spike_flux compare
"""
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests

OUT = Path("data/spike")


def snapshot() -> Path:
    OUT.mkdir(parents=True, exist_ok=True)
    r = requests.get(os.environ["FLUX_EXPORT_URL"], timeout=120)
    r.raise_for_status()
    df = pd.json_normalize(r.json())
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    path = OUT / f"snap_{ts}.csv"
    df.to_csv(path, index=False)
    print(f"{len(df)} lignes -> {path}")
    print("colonnes :", list(df.columns))
    return path


def diff_snapshots(a: pd.DataFrame, b: pd.DataFrame, key: str = "id") -> dict:
    """Compare deux snapshots et compte les changements par colonne."""
    a = a.astype(str)
    b = b.astype(str)
    merged = a.merge(b, on=key, suffixes=("_a", "_b"), how="outer", indicator=True)
    both = merged[merged["_merge"] == "both"]
    changes = {}
    for col in a.columns:
        if col == key or col not in b.columns:
            continue
        diff = both[f"{col}_a"].fillna("") != both[f"{col}_b"].fillna("")
        changes[col] = int(diff.sum())
    return {
        "lignes_a": len(a),
        "lignes_b": len(b),
        "doublons_a": int(a[key].duplicated().sum()),
        "presents_both": int((merged["_merge"] == "both").sum()),
        "seulement_a": int((merged["_merge"] == "left_only").sum()),
        "seulement_b": int((merged["_merge"] == "right_only").sum()),
        "changements_par_colonne": changes,
    }


def compare(key: str = os.environ.get("SPIKE_KEY", "id")) -> None:
    files = sorted(OUT.glob("snap_*.csv"))
    if len(files) < 2:
        sys.exit("Il faut au moins deux snapshots (attendre 10 min entre deux).")
    a, b = (pd.read_csv(f, dtype=str) for f in files[-2:])
    print("comparaison :", files[-2].name, "vs", files[-1].name)
    for k, v in diff_snapshots(a, b, key).items():
        print(f"{k} : {v}")


if __name__ == "__main__":
    if len(sys.argv) != 2 or sys.argv[1] not in {"snapshot", "compare"}:
        sys.exit("Usage : python -m ingestion.spike_flux [snapshot|compare]")
    snapshot() if sys.argv[1] == "snapshot" else compare()
