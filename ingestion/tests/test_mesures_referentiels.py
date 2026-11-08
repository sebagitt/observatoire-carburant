from ingestion.mesures_referentiels import communes_sans_ligne, compter_par_departement, departement_de, part_vide


def test_departement_de_gere_l_outre_mer_a_trois_chiffres_et_la_corse():
    assert departement_de("01001") == "01"
    assert departement_de("97101") == "971"
    assert departement_de("97611") == "976"
    assert departement_de("2A004") == "2A"
    assert departement_de("2B033") == "2B"


def test_communes_sans_ligne_est_la_difference_triee():
    assert communes_sans_ligne({"01001", "01002", "97101"}, {"01001"}) == ["01002", "97101"]
    assert communes_sans_ligne({"01001"}, {"01001", "99999"}) == []


def test_compter_par_departement_trie_par_effectif_decroissant_puis_code():
    r = compter_par_departement(["97101", "97102", "97201", "29083", "29084", "29085"])
    assert list(r.items()) == [("29", 3), ("971", 2), ("972", 1)]


def test_part_vide_en_pourcentage_et_zero_sans_effectif():
    assert part_vide(29526, 34768) == round(100 * 29526 / 34768, 1)
    assert part_vide(0, 0) == 0.0
