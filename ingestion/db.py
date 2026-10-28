"""Tampon Postgres append-only : prix_flux (dédoublonné sur la clé) et station_etat_flux (versions d'état)."""
from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

SQL_DIR = Path(__file__).parent / "sql"


def connecter(dsn: str | None = None):
    return psycopg2.connect(dsn or os.environ["COLLECTE_DSN"])


def ensure_schema(conn) -> None:
    with conn.cursor() as cur:
        for fichier in sorted(SQL_DIR.glob("*.sql")):
            cur.execute(fichier.read_text(encoding="utf-8"))
    conn.commit()


def charger_prix(conn, df: pd.DataFrame, collecte_ts: datetime) -> dict:
    """Insère le lot, ignore les clés déjà vues. Renvoie les compteurs du passage."""
    lignes = [
        (int(r.station_id), r.carburant, r.maj.to_pydatetime(), round(float(r.prix), 3), collecte_ts)
        for r in df.itertuples(index=False)
    ]
    with conn.cursor() as cur:
        cur.execute("CREATE TEMP TABLE lot (LIKE prix_flux INCLUDING DEFAULTS) ON COMMIT DROP")
        execute_values(
            cur,
            "INSERT INTO lot (station_id, carburant, maj, prix, collecte_ts) VALUES %s",
            lignes,
            page_size=10_000,
        )
        # Même clé, prix différent : ON CONFLICT DO NOTHING l'ignorerait en silence, on le compte.
        cur.execute(
            """
            SELECT count(*) FROM lot l
            JOIN prix_flux p USING (station_id, carburant, maj)
            WHERE p.prix IS DISTINCT FROM l.prix
            """
        )
        conflits_prix = cur.fetchone()[0]
        cur.execute(
            """
            INSERT INTO prix_flux (station_id, carburant, maj, prix, collecte_ts)
            SELECT station_id, carburant, maj, prix, collecte_ts FROM lot
            ON CONFLICT (station_id, carburant, maj) DO NOTHING
            """
        )
        inserees = cur.rowcount
    conn.commit()
    return {
        "lot": len(lignes),
        "inserees": inserees,
        "deja_vues": len(lignes) - inserees,
        "conflits_prix": conflits_prix,
    }


class EtatsInstablesError(ValueError):
    """Trop de stations changent d'état d'un coup : champ volatil probable, rien n'est écrit."""


def charger_etats(conn, df: pd.DataFrame, collecte_ts: datetime, max_part_changee: float = 0.3) -> dict:
    """Écrit une nouvelle version d'état pour les stations nouvelles ou dont l'état a changé.

    Garde-fou : si plus de max_part_changee des stations déjà connues changent en un seul passage,
    on lève EtatsInstablesError sans rien écrire (sinon un champ volatil gonflerait la table).
    """
    lignes = [(int(r.station_id), collecte_ts, r.etat_hash, r.etat) for r in df.itertuples(index=False)]
    try:
        with conn.cursor() as cur:
            cur.execute(
                "CREATE TEMP TABLE lot_etat (station_id BIGINT, collecte_ts TIMESTAMPTZ, "
                "etat_hash TEXT, etat JSONB) ON COMMIT DROP"
            )
            execute_values(
                cur,
                "INSERT INTO lot_etat (station_id, collecte_ts, etat_hash, etat) VALUES %s",
                lignes,
                template="(%s, %s, %s, %s::jsonb)",
                page_size=5_000,
            )
            cur.execute(
                """
                CREATE TEMP TABLE dernier_etat ON COMMIT DROP AS
                SELECT DISTINCT ON (station_id) station_id, etat_hash, etat
                FROM station_etat_flux
                WHERE station_id IN (SELECT station_id FROM lot_etat)
                ORDER BY station_id, collecte_ts DESC
                """
            )
            cur.execute("SELECT count(*) FROM dernier_etat")
            connues = cur.fetchone()[0]
            cur.execute(
                "SELECT count(*) FROM lot_etat l JOIN dernier_etat d USING (station_id) "
                "WHERE d.etat_hash <> l.etat_hash"
            )
            changees = cur.fetchone()[0]
            if connues and changees / connues > max_part_changee:
                cur.execute(
                    """
                    SELECT k, count(*) FROM lot_etat l JOIN dernier_etat d USING (station_id)
                    CROSS JOIN LATERAL jsonb_object_keys(l.etat) AS k
                    WHERE d.etat_hash <> l.etat_hash AND (l.etat -> k) IS DISTINCT FROM (d.etat -> k)
                    GROUP BY k ORDER BY 2 DESC LIMIT 3
                    """
                )
                suspects = ", ".join(f"{cle} ({n})" for cle, n in cur.fetchall())
                raise EtatsInstablesError(
                    f"{changees} stations sur {connues} ont changé d'état en un passage "
                    f"(seuil {max_part_changee:.0%}) : champ volatil probable, rien n'est écrit. "
                    f"Clés les plus changeantes : {suspects}"
                )
            cur.execute(
                """
                INSERT INTO station_etat_flux (station_id, collecte_ts, etat_hash, etat)
                SELECT l.station_id, l.collecte_ts, l.etat_hash, l.etat
                FROM lot_etat l LEFT JOIN dernier_etat d USING (station_id)
                WHERE d.etat_hash IS DISTINCT FROM l.etat_hash
                ON CONFLICT (station_id, collecte_ts) DO NOTHING
                """
            )
            inserees = cur.rowcount
    except Exception:
        conn.rollback()
        raise
    conn.commit()
    return {"lot": len(lignes), "inserees": inserees, "nouvelles": len(lignes) - connues, "changees": changees}
