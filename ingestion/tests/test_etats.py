import json

from ingestion.carburants import construire_etats


def station(**kw):
    base = {
        "id": 1,
        "adresse": "1 rue A",
        "ville": "Nantes",
        "services_service": ["Lavage", "Boutique"],
        "carburants_rupture_temporaire": None,
        "gazole_prix": 1.9,
        "gazole_maj": "2026-10-23T10:00:00+00:00",
        "prix": "[1.9]",
    }
    base.update(kw)
    return base


def hash_de(**kw):
    return construire_etats([station(**kw)]).loc[0, "etat_hash"]


def test_colonnes_et_etat_en_json():
    df = construire_etats([station()])
    assert list(df.columns) == ["station_id", "etat_hash", "etat"]
    assert json.loads(df.loc[0, "etat"])["ville"] == "Nantes"


def test_changement_de_prix_ne_change_pas_l_etat():
    autre = hash_de(gazole_prix=2.1, gazole_maj="2026-10-23T11:00:00+00:00", prix="[2.1]")
    assert autre == hash_de()


def test_changement_d_attribut_change_l_etat():
    assert hash_de(adresse="2 rue B") != hash_de()


def test_apparition_d_une_rupture_change_l_etat():
    assert hash_de(carburants_rupture_temporaire="Gazole") != hash_de()


def test_ordre_des_listes_sans_effet():
    assert hash_de(services_service=["Boutique", "Lavage"]) == hash_de()


def test_etat_ne_contient_ni_prix_ni_maj_ni_id():
    etat = json.loads(construire_etats([station()]).loc[0, "etat"])
    assert not any(k in etat for k in ("id", "prix", "gazole_prix", "gazole_maj"))


def test_station_sans_id_ignoree_et_flux_vide():
    assert construire_etats([{"adresse": "x"}]).empty
    assert list(construire_etats([]).columns) == ["station_id", "etat_hash", "etat"]
