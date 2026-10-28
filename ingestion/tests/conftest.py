import os
import sys
import uuid
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))


@pytest.fixture
def conn():
    """Connexion Postgres isolée dans un schéma temporaire (la vraie table n'est jamais touchée)."""
    dsn = os.environ.get("COLLECTE_DSN")
    if not dsn:
        pytest.skip("COLLECTE_DSN non défini")
    import psycopg2

    connexion = psycopg2.connect(dsn)
    schema = f"test_{uuid.uuid4().hex[:8]}"
    with connexion.cursor() as cur:
        cur.execute(f"CREATE SCHEMA {schema}")
        cur.execute(f"SET search_path TO {schema}")
    connexion.commit()
    yield connexion
    connexion.rollback()
    with connexion.cursor() as cur:
        cur.execute(f"DROP SCHEMA {schema} CASCADE")
    connexion.commit()
    connexion.close()
