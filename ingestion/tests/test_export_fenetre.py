import io
import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from ingestion.db import ensure_schema
from ingestion.export_flux import exporter_fenetre

UTC = timezone.utc


def t(jour, heure, minute=0, seconde=0):
    return datetime(2026, 11, jour, heure, minute, seconde, tzinfo=UTC)


@pytest.fixture
def base(conn):
    ensure_schema(conn)
    return conn


def prix(conn, station_id, collecte_ts, carburant="Gazole", maj=None, valeur="1.234"):
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO prix_flux (station_id, carburant, maj, prix, collecte_ts) VALUES (%s, %s, %s, %s, %s)",
            (station_id, carburant, maj or collecte_ts, valeur, collecte_ts),
        )
    conn.commit()


def etat(conn, station_id, collecte_ts, json_texte="{}", etat_hash="h"):
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO station_etat_flux (station_id, collecte_ts, etat_hash, etat) VALUES (%s, %s, %s, %s::jsonb)",
            (station_id, collecte_ts, etat_hash, json_texte),
        )
    conn.commit()


def lire(octets):
    return pq.read_table(io.BytesIO(octets))


SCHEMA_PRIX = pa.schema(
    [
        ("station_id", pa.int64()),
        ("carburant", pa.string()),
        ("maj", pa.timestamp("us", tz="UTC")),
        ("prix", pa.decimal128(6, 3)),
        ("collecte_ts", pa.timestamp("us", tz="UTC")),
    ]
)
SCHEMA_ETAT = pa.schema(
    [
        ("station_id", pa.int64()),
        ("collecte_ts", pa.timestamp("us", tz="UTC")),
        ("etat_hash", pa.string()),
        ("etat", pa.string()),
    ]
)


def test_prix_bornes_semi_ouvertes(base):
    prix(base, 1, t(12, 9, 59, 59))  # avant : exclue
    prix(base, 2, t(12, 10, 0, 0))  # exactement debut : incluse
    prix(base, 3, t(12, 10, 30))  # dedans
    prix(base, 4, t(12, 11, 0, 0))  # exactement fin : exclue
    prix(base, 5, t(12, 11, 0, 1))  # après : exclue
    table = lire(exporter_fenetre(base, "prix_flux", t(12, 10), t(12, 11)))
    assert table.column("station_id").to_pylist() == [2, 3]


def test_etats_bornes_semi_ouvertes(base):
    etat(base, 1, t(12, 9, 59, 59))
    etat(base, 2, t(12, 10, 0, 0))
    etat(base, 3, t(12, 10, 30))
    etat(base, 4, t(12, 11, 0, 0))
    etat(base, 5, t(12, 11, 0, 1))
    table = lire(exporter_fenetre(base, "station_etat_flux", t(12, 10), t(12, 11)))
    assert table.column("station_id").to_pylist() == [2, 3]


def test_ordre_par_cle_primaire_quel_que_soit_l_ordre_d_insertion(base):
    ts = t(12, 10, 5)
    prix(base, 7, ts, "Gazole", t(12, 9))
    prix(base, 3, ts, "SP95", t(12, 9))
    prix(base, 3, ts, "Gazole", t(12, 9, 30))
    prix(base, 3, ts, "Gazole", t(12, 9))
    table = lire(exporter_fenetre(base, "prix_flux", t(12, 10), t(12, 11)))
    cles = list(zip(table.column("station_id").to_pylist(), table.column("carburant").to_pylist(),
                    table.column("maj").to_pylist()))
    assert cles == [(3, "Gazole", t(12, 9)), (3, "Gazole", t(12, 9, 30)), (3, "SP95", t(12, 9)),
                    (7, "Gazole", t(12, 9))]


def test_ordre_des_etats_par_station_puis_collecte_ts(base):
    etat(base, 9, t(12, 10, 20))
    etat(base, 2, t(12, 10, 40))
    etat(base, 2, t(12, 10, 10))
    table = lire(exporter_fenetre(base, "station_etat_flux", t(12, 10), t(12, 11)))
    assert list(zip(table.column("station_id").to_pylist(), table.column("collecte_ts").to_pylist())) == [
        (2, t(12, 10, 10)), (2, t(12, 10, 40)), (9, t(12, 10, 20)),
    ]


def test_schema_parquet_des_prix_et_valeurs(base):
    paris = timezone(timedelta(hours=2))
    prix(base, 1, datetime(2026, 11, 12, 12, 30, tzinfo=paris), maj=datetime(2026, 11, 12, 12, 0, tzinfo=paris),
         valeur="1.5")
    table = lire(exporter_fenetre(base, "prix_flux", t(12, 10), t(12, 11)))
    assert table.schema.equals(SCHEMA_PRIX)
    ligne = table.to_pylist()[0]
    assert ligne["prix"] == Decimal("1.500")
    assert ligne["maj"] == t(12, 10)  # 12:00 à +02:00 est 10:00 UTC
    assert ligne["collecte_ts"] == t(12, 10, 30)


def test_schema_parquet_des_etats(base):
    etat(base, 1, t(12, 10, 30))
    table = lire(exporter_fenetre(base, "station_etat_flux", t(12, 10), t(12, 11)))
    assert table.schema.equals(SCHEMA_ETAT)


def test_etat_en_texte_canonique_de_postgres(base):
    source = '{"ville": "Saint-Étienne", "adresse": "Rue de l\'Éléphant, café", "prix": 1.10, "a": {"z": [1.50, "é"], "b": 2}}'
    etat(base, 1, t(12, 10, 30), source)
    with base.cursor() as cur:
        cur.execute("SELECT etat::text FROM station_etat_flux")
        attendu = cur.fetchone()[0]
    base.rollback()
    texte = lire(exporter_fenetre(base, "station_etat_flux", t(12, 10), t(12, 11))).column("etat").to_pylist()[0]
    assert texte == attendu
    assert "Éléphant" in texte and "café" in texte  # accents conservés, pas de é
    assert "\\u" not in texte
    assert "1.10" in texte and "1.50" in texte  # précision des nombres conservée
    assert json.loads(texte)["a"]["b"] == 2  # clé imbriquée, JSON valide


@pytest.mark.parametrize("table", ["prix_flux", "station_etat_flux"])
def test_fenetre_vide_donne_un_parquet_valide_au_bon_schema(base, table):
    lu = lire(exporter_fenetre(base, table, t(12, 10), t(12, 11)))
    assert lu.num_rows == 0
    assert lu.schema.equals(SCHEMA_PRIX if table == "prix_flux" else SCHEMA_ETAT)


def test_compression_snappy(base):
    prix(base, 1, t(12, 10, 30))
    octets = exporter_fenetre(base, "prix_flux", t(12, 10), t(12, 11))
    assert pq.ParquetFile(io.BytesIO(octets)).metadata.row_group(0).column(0).compression == "SNAPPY"


@pytest.mark.parametrize("table", ["prix_flux", "station_etat_flux"])
def test_memes_donnees_memes_octets(base, table):
    prix(base, 1, t(12, 10, 30))
    prix(base, 2, t(12, 10, 45), "SP98")
    etat(base, 1, t(12, 10, 30), '{"adresse": "Café"}')
    etat(base, 2, t(12, 10, 45), '{"n": 1.10}')
    premier = exporter_fenetre(base, table, t(12, 10), t(12, 11))
    deuxieme = exporter_fenetre(base, table, t(12, 10), t(12, 11))
    assert premier == deuxieme
    prix(base, 3, t(12, 11, 30))  # hors fenêtre : ne change rien
    etat(base, 3, t(12, 11, 30))
    assert exporter_fenetre(base, table, t(12, 10), t(12, 11)) == premier


def test_borne_sans_fuseau_refusee(base):
    with pytest.raises(ValueError):
        exporter_fenetre(base, "prix_flux", datetime(2026, 11, 12, 10), t(12, 11))
    with pytest.raises(ValueError):
        exporter_fenetre(base, "prix_flux", t(12, 10), datetime(2026, 11, 12, 11))


@pytest.mark.parametrize("table", ["passage_flux", "export_curseur", "prix_flux; DROP TABLE prix_flux", ""])
def test_table_hors_liste_blanche_refusee(base, table):
    with pytest.raises(ValueError):
        exporter_fenetre(base, table, t(12, 10), t(12, 11))
