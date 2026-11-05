import json

from ingestion.carburants import _normaliser, construire_etats

SERVICES = '{"service": ["Bar", "Boutique alimentaire", "Lavage automatique"]}'
SERVICES_REORDONNES = '{"service": ["Lavage automatique", "Bar", "Boutique alimentaire"]}'
RUPTURE = (
    '[{"@nom": "SP95", "@id": "2", "@debut": "2025-10-06 09:21:00", "@fin": "", "@type": "temporaire"},'
    ' {"@nom": "E85", "@id": "3", "@debut": "2025-10-07 08:00:00", "@fin": "", "@type": "temporaire"}]'
)
RUPTURE_REORDONNEE = (
    '[{"@nom": "E85", "@id": "3", "@debut": "2025-10-07 08:00:00", "@fin": "", "@type": "temporaire"},'
    ' {"@nom": "SP95", "@id": "2", "@debut": "2025-10-06 09:21:00", "@fin": "", "@type": "temporaire"}]'
)
HORAIRES = (
    '{"@automate-24-24": "1", "jour": [{"@id": "1", "@nom": "Lundi", "@ferme": ""},'
    ' {"@id": "2", "@nom": "Mardi", "@ferme": "1"}]}'
)
HORAIRES_REORDONNES = (
    '{"@automate-24-24": "1", "jour": [{"@id": "2", "@nom": "Mardi", "@ferme": "1"},'
    ' {"@id": "1", "@nom": "Lundi", "@ferme": ""}]}'
)


def station(**kw):
    base = {"id": 1, "adresse": "1 rue A", "services": SERVICES, "rupture": RUPTURE, "horaires": HORAIRES}
    base.update(kw)
    return base


def hash_de(**kw):
    return construire_etats([station(**kw)]).loc[0, "etat_hash"]


def test_ordre_des_services_dans_la_chaine_json_sans_effet():
    assert hash_de(services=SERVICES_REORDONNES) == hash_de()
    assert hash_de(services='{"service": ["Bar", "Lavage automatique"]}') != hash_de()


def test_ordre_des_ruptures_dans_la_chaine_json_sans_effet():
    assert hash_de(rupture=RUPTURE_REORDONNEE) == hash_de()
    assert hash_de(rupture=RUPTURE.replace('"@fin": ""', '"@fin": "2025-11-01 10:00:00"', 1)) != hash_de()


def test_ordre_des_jours_d_horaires_sans_effet():
    assert hash_de(horaires=HORAIRES_REORDONNES) == hash_de()
    assert hash_de(horaires=HORAIRES.replace('"@ferme": "1"', '"@ferme": ""')) != hash_de()


def test_chaine_non_json_ou_scalaire_reste_intacte():
    assert _normaliser("[Total] 1 rue A") == "[Total] 1 rue A"
    assert _normaliser("{incomplet") == "{incomplet"
    assert _normaliser("123") == "123"
    assert hash_de(adresse="[Total] 1 rue A") != hash_de(adresse="[Total] 2 rue B")


def test_l_etat_garde_la_chaine_d_origine():
    etat = json.loads(construire_etats([station(services=SERVICES_REORDONNES)]).loc[0, "etat"])
    assert etat["services"] == SERVICES_REORDONNES
