from datetime import datetime, timedelta, timezone

from ingestion.mesures_carburants import classer_disparitions, est_vide, lots_ecartes, resume, taux_nulls

T0 = datetime(2026, 10, 30, 12, 0, tzinfo=timezone.utc)


def minutes(*valeurs):
    return [T0 + timedelta(minutes=v) for v in valeurs]


def test_vide_signifie_absent_null_chaine_blanche_liste_ou_dict_vide():
    assert all(est_vide(v) for v in (None, "", "  ", [], {}))
    assert not any(est_vide(v) for v in (0, "0", "x", [1], {"a": 1}, False))


def test_taux_nulls_compte_un_champ_absent_comme_vide():
    stations = [{"a": "x", "b": ""}, {"a": None}, {"a": "y", "b": "z", "c": 1}]
    r = taux_nulls(stations)
    assert r["a"]["non_vides"] == 2 and round(r["a"]["taux_vide"], 3) == 0.333
    assert r["b"]["non_vides"] == 1
    assert r["c"] == {"non_vides": 1, "taux_vide": 1 - 1 / 3, "type": "int"}


def test_lots_ecartes_premier_passage_et_ecart_strictement_superieur_a_20_minutes():
    # 0 puis +15, +15 (normaux), +20 pile (non écarté), +21 (écarté), +15 (normal)
    passages = minutes(0, 15, 30, 50, 71, 86)
    ecartes = lots_ecartes(passages)
    assert [ts for ts, _ in ecartes] == minutes(0, 71)
    assert ecartes[0][1] is None and ecartes[1][1] == timedelta(minutes=21)


def test_lots_ecartes_ignore_l_ordre_et_les_doublons():
    assert [ts for ts, _ in lots_ecartes(minutes(40, 0, 0))] == minutes(0, 40)


def test_resume_mediane_et_p95_par_rang():
    assert resume(range(1, 101)) == {"n": 100, "moyenne": 50.5, "mediane": 50.5, "p95": 95}
    assert resume([]) == {"n": 0, "moyenne": None, "mediane": None, "p95": None}
    assert resume([7])["p95"] == 7


def entree(carburant, debut, type_="temporaire"):
    return {"@id": carburant, "@debut": debut, "@fin": "", "@type": type_}


def test_classer_disparitions_quatre_categories():
    v = [
        ("t1", [entree("1", "2026-10-01"), entree("2", "2026-10-02"), entree("3", "2026-10-03"), entree("4", "2026-10-04")]),
        # 1 disparaît sans retour, 2 est remplacée par un @debut plus récent, 3 par un plus ancien, 4 disparaît puis revient
        ("t2", [entree("2", "2026-10-09"), entree("3", "2026-09-01")]),
        ("t3", [entree("2", "2026-10-09"), entree("3", "2026-09-01"), entree("4", "2026-10-20")]),
    ]
    assert sorted(classer_disparitions(v)) == sorted(
        [
            ("fin_probable", "temporaire"),
            ("autre_debut_plus_recent", "temporaire"),
            ("autre_debut_plus_ancien", "temporaire"),
            ("reapparait_plus_tard", "temporaire"),
        ]
    )


def test_classer_disparitions_rien_si_les_entrees_restent():
    v = [("t1", [entree("1", "2026-10-01")]), ("t2", [entree("1", "2026-10-01")])]
    assert classer_disparitions(v) == []
