"""Export d'une fenêtre [début, fin[ d'une table source en Parquet construit en mémoire."""
from __future__ import annotations

import io
from datetime import datetime

import pyarrow as pa
import pyarrow.parquet as pq

from ingestion.export_curseur import TABLES_EXPORTABLES

_HORODATAGE = pa.timestamp("us", tz="UTC")

# Requêtes fixes par table de la liste blanche : aucune interpolation de nom.
# etat::text est le texte canonique de Postgres (accents et précision des nombres conservés).
_EXPORTS = {
    "prix_flux": (
        "SELECT station_id, carburant, maj, prix, collecte_ts FROM prix_flux "
        "WHERE collecte_ts >= %s AND collecte_ts < %s "
        "ORDER BY station_id, carburant, maj",
        pa.schema(
            [
                ("station_id", pa.int64()),
                ("carburant", pa.string()),
                ("maj", _HORODATAGE),
                ("prix", pa.decimal128(6, 3)),
                ("collecte_ts", _HORODATAGE),
            ]
        ),
    ),
    "station_etat_flux": (
        "SELECT station_id, collecte_ts, etat_hash, etat::text FROM station_etat_flux "
        "WHERE collecte_ts >= %s AND collecte_ts < %s "
        "ORDER BY station_id, collecte_ts",
        pa.schema(
            [
                ("station_id", pa.int64()),
                ("collecte_ts", _HORODATAGE),
                ("etat_hash", pa.string()),
                ("etat", pa.string()),
            ]
        ),
    ),
}
assert set(_EXPORTS) == set(TABLES_EXPORTABLES)


def exporter_fenetre(conn, table_source: str, debut: datetime, fin: datetime) -> bytes:
    """Parquet (snappy) des lignes avec debut <= collecte_ts < fin, triées par clé primaire.

    Mêmes données, mêmes octets. Fenêtre vide : Parquet valide à zéro ligne, bon schéma."""
    if table_source not in _EXPORTS:
        raise ValueError(f"table non exportable : {table_source!r}")
    for nom, valeur in (("debut", debut), ("fin", fin)):
        if valeur.tzinfo is None or valeur.utcoffset() is None:
            raise ValueError(f"{nom} doit avoir un fuseau")
    requete, schema = _EXPORTS[table_source]
    with conn.cursor() as cur:
        cur.execute(requete, (debut, fin))
        lignes = cur.fetchall()
    conn.rollback()  # lecture seule : ne laisse pas de transaction ouverte
    colonnes = [pa.array([ligne[i] for ligne in lignes], type=champ.type) for i, champ in enumerate(schema)]
    tampon = io.BytesIO()
    pq.write_table(pa.Table.from_arrays(colonnes, schema=schema), tampon, compression="snappy")
    return tampon.getvalue()
