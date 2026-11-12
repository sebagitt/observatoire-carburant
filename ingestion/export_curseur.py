"""Curseur d'export : borne haute déjà exportée, une ligne par table source et par source."""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

SQL_EXPORT_DIR = Path(__file__).parent / "sql_export"

# Liste blanche : requêtes fixes, aucun nom de table n'est jamais interpolé dans du SQL.
TABLES_EXPORTABLES = {
    "prix_flux": "SELECT min(collecte_ts) FROM prix_flux",
    "station_etat_flux": "SELECT min(collecte_ts) FROM station_etat_flux",
}


class CurseurPerimeError(RuntimeError):
    """Le curseur n'a pas la valeur attendue (changé entre-temps, ou ligne absente)."""


def _verifier_table(table_source: str) -> None:
    if table_source not in TABLES_EXPORTABLES:
        raise ValueError(f"table non exportable : {table_source!r}")


def _verifier_fuseau(valeur: datetime, nom: str) -> None:
    if valeur.tzinfo is None or valeur.utcoffset() is None:
        raise ValueError(f"{nom} doit avoir un fuseau")


def creer_table_curseur(conn) -> None:
    """Crée export_curseur (rejouable). Jamais appelée par ensure_schema."""
    with conn.cursor() as cur:
        cur.execute((SQL_EXPORT_DIR / "export_curseur.sql").read_text(encoding="utf-8"))
    conn.commit()


def lire_curseur(conn, table_source: str, source: str) -> datetime | None:
    """Curseur courant, ou None si la ligne n'existe pas."""
    _verifier_table(table_source)
    with conn.cursor() as cur:
        cur.execute(
            "SELECT curseur FROM export_curseur WHERE table_source = %s AND source = %s",
            (table_source, source),
        )
        ligne = cur.fetchone()
    conn.rollback()  # lecture seule : ne laisse pas de transaction ouverte
    return None if ligne is None else ligne[0].astimezone(timezone.utc)


def initialiser_curseur(conn, table_source: str, source: str) -> datetime | None:
    """Crée la ligne au plancher horaire UTC du plus ancien collecte_ts, si elle n'existe pas.

    Ne modifie jamais une ligne existante (la renvoie telle quelle). Table source vide : None."""
    _verifier_table(table_source)
    existant = lire_curseur(conn, table_source, source)
    if existant is not None:
        return existant
    with conn.cursor() as cur:
        cur.execute(TABLES_EXPORTABLES[table_source])
        minimum = cur.fetchone()[0]
        if minimum is None:
            conn.rollback()
            return None
        plancher = minimum.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)
        cur.execute(
            "INSERT INTO export_curseur (table_source, source, curseur) VALUES (%s, %s, %s) "
            "ON CONFLICT (table_source, source) DO NOTHING",
            (table_source, source, plancher),
        )
    conn.commit()
    return lire_curseur(conn, table_source, source)


def avancer_curseur(conn, table_source: str, source: str, ancien: datetime, nouveau: datetime) -> None:
    """Comparer-et-remplacer : n'avance que si le curseur vaut encore `ancien`."""
    _verifier_table(table_source)
    _verifier_fuseau(ancien, "ancien")
    _verifier_fuseau(nouveau, "nouveau")
    if nouveau <= ancien:
        raise ValueError("nouveau doit être postérieur à ancien")
    if nouveau.minute or nouveau.second or nouveau.microsecond:
        raise ValueError("nouveau doit être aligné sur l'heure")
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE export_curseur SET curseur = %s, mis_a_jour = now() "
            "WHERE table_source = %s AND source = %s AND curseur = %s",
            (nouveau, table_source, source, ancien),
        )
        modifiees = cur.rowcount
    if modifiees != 1:
        conn.rollback()
        raise CurseurPerimeError(
            f"curseur de {table_source}/{source} différent de {ancien.isoformat()} : rien n'est modifié"
        )
    conn.commit()
