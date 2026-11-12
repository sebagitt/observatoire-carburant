from datetime import datetime, timedelta, timezone

import pytest

from ingestion.fenetres import fenetres_a_exporter, nom_fichier

UTC = timezone.utc


def t(jour, heure, minute=0, seconde=0):
    return datetime(2026, 11, jour, heure, minute, seconde, tzinfo=UTC)


def test_fenetres_alignees_sur_l_heure_et_semi_ouvertes():
    assert fenetres_a_exporter(t(12, 10), t(12, 14, 30)) == [
        (t(12, 10), t(12, 11)),
        (t(12, 11), t(12, 12)),
    ]


def test_une_fenetre_n_est_close_que_deux_heures_apres_sa_fin():
    assert fenetres_a_exporter(t(12, 10), t(12, 13, 59, 59)) == [(t(12, 10), t(12, 11))]
    assert fenetres_a_exporter(t(12, 10), t(12, 14, 0, 0)) == [
        (t(12, 10), t(12, 11)),
        (t(12, 11), t(12, 12)),
    ]


def test_rien_a_exporter_si_aucune_fenetre_close():
    assert fenetres_a_exporter(t(12, 12), t(12, 13, 30)) == []


def test_marge_parametrable():
    assert fenetres_a_exporter(t(12, 10), t(12, 13, 30), marge=timedelta(hours=1)) == [
        (t(12, 10), t(12, 11)),
        (t(12, 11), t(12, 12)),
    ]


def test_rattrapage_apres_un_long_arret():
    res = fenetres_a_exporter(t(9, 16), t(12, 14, 30))
    assert len(res) == 68
    assert res[0] == (t(9, 16), t(9, 17))
    assert res[-1] == (t(12, 11), t(12, 12))
    assert all(a[1] == b[0] for a, b in zip(res, res[1:]))


def test_curseur_non_aligne_ou_sans_fuseau_refuse():
    with pytest.raises(ValueError):
        fenetres_a_exporter(t(12, 10, 30), t(12, 20))
    with pytest.raises(ValueError):
        fenetres_a_exporter(datetime(2026, 11, 12, 10), t(12, 20))
    with pytest.raises(ValueError):
        fenetres_a_exporter(t(12, 10), datetime(2026, 11, 12, 20))


def test_nom_de_fichier_deterministe():
    assert (
        nom_fichier("prix_flux", "instantane", t(12, 10), t(12, 11))
        == "prix_flux/instantane/20261112T100000Z__20261112T110000Z.parquet"
    )
    