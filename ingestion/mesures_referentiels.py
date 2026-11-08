"""Mesures sur les référentiels chargés (lecture seule), dans le conteneur : python -m ingestion.mesures_referentiels filosofi

filosofi : couverture des codes de ref_filosofi_commune dans ref_cog_commune par TYPECOM (Paris, Lyon, Marseille : commune ou
arrondissement), communes du COG sans ligne Filosofi, part de valeurs vides de PR_MD60 et MED_SL, les 7 communes annoncées sans données.
Prérequis : make charger-cog puis make charger-filosofi.
"""
from __future__ import annotations

import argparse
import collections

from ingestion.db import connecter
from ingestion.mesures_carburants import couverture_codes

# Communes citées par la page Filosofi comme sans données (incluses dans une commune nouvelle) : documenté.
SEPT_COMMUNES = ("15031", "15035", "15047", "15171", "85165", "85212", "09304")
VILLES = (("Paris", "75056", "751"), ("Lyon", "69123", "6938"), ("Marseille", "13055", "132"))  # (ville, commune, préfixe des arrondissements)


def departement_de(code: str) -> str:
    """Département d'un code commune : trois chiffres pour l'outre-mer (97x), deux caractères sinon (dont 2A et 2B)."""
    return code[:3] if code.startswith("97") else code[:2]


def communes_sans_ligne(codes_cog: set[str], codes_filosofi: set[str]) -> list[str]:
    return sorted(set(codes_cog) - set(codes_filosofi))


def compter_par_departement(codes) -> dict[str, int]:
    compte = collections.Counter(departement_de(c) for c in codes)
    return dict(sorted(compte.items(), key=lambda x: (-x[1], x[0])))


def part_vide(vides: int, total: int) -> float:
    return round(100 * vides / total, 1) if total else 0.0


def filosofi() -> None:
    conn = connecter()
    conn.set_session(readonly=True)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT com, array_agg(DISTINCT typecom) FROM ref_cog_commune WHERE millesime = 2026 GROUP BY com")
            types_par_code = {com: set(t) for com, t in cur.fetchall()}
            cur.execute("SELECT com FROM ref_cog_commune WHERE millesime = 2026 AND typecom = 'COM'")
            cog_com = {r[0] for r in cur.fetchall()}
            cur.execute("SELECT geo_object, codgeo FROM ref_filosofi_commune WHERE millesime = 2023")
            geos = collections.defaultdict(list)
            for niveau, code in cur.fetchall():
                geos[niveau].append(code)
            cur.execute(
                "SELECT geo_object, count(*), count(*) FILTER (WHERE med_sl IS NULL), count(*) FILTER (WHERE pr_md60 IS NULL) "
                "FROM ref_filosofi_commune WHERE millesime = 2023 GROUP BY 1 ORDER BY 1"
            )
            vides = cur.fetchall()
            cur.execute(
                "SELECT med_sl_statut, med_sl_conf, pr_md60_statut, pr_md60_conf, count(*) FROM ref_filosofi_commune "
                "WHERE millesime = 2023 AND geo_object = 'COM' GROUP BY 1, 2, 3, 4 ORDER BY 5 DESC"
            )
            statuts = cur.fetchall()
            cur.execute(
                "SELECT codgeo, med_sl, med_sl_statut, pr_md60, pr_md60_statut FROM ref_filosofi_commune "
                "WHERE millesime = 2023 AND geo_object = 'COM' AND codgeo = ANY(%s) ORDER BY codgeo",
                (list(SEPT_COMMUNES) + [v[1] for v in VILLES],),
            )
            cas = {r[0]: r[1:] for r in cur.fetchall()}
    finally:
        conn.close()
    print("## Filosofi 2023 : couverture dans ref_cog_commune (COG 2026) et valeurs vides")
    for niveau in ("COM", "ARM"):
        r = couverture_codes(geos.get(niveau, []), types_par_code)
        print(f"  niveau {niveau} : {r['codes_distincts']} codes ; type dans le COG : {r['codes_par_categorie']} ; absents du COG : {r['codes_absents'][:10]}")
    manque = communes_sans_ligne(cog_com, set(geos.get("COM", [])))
    print(f"  communes COM du COG sans aucune ligne Filosofi : {len(manque)} ; par département : {compter_par_departement(manque)}")
    print(f"  codes Filosofi COM qui ne sont pas COM dans le COG : {sorted(set(geos.get('COM', [])) - cog_com)[:10]}")
    for niveau, total, vide_med, vide_pr in vides:
        print(f"  {niveau} : {total} zones ; MED_SL vide {vide_med} ({part_vide(vide_med, total)} %) ; PR_MD60 vide {vide_pr} ({part_vide(vide_pr, total)} %)")
    print("  (statut MED_SL, confidentialité MED_SL, statut PR_MD60, confidentialité PR_MD60) : nombre de communes")
    for s in statuts:
        print(f"    {s[:4]} : {s[4]}")
    print("  les 7 communes annoncées sans données :")
    for code in SEPT_COMMUNES:
        print(f"    {code} dans le COG : {code in cog_com} ; ligne Filosofi (MED_SL, statut, PR_MD60, statut) : {cas.get(code)}")
    for ville, code, prefixe in VILLES:
        arrondissements = sorted(c for c in geos.get("ARM", []) if c.startswith(prefixe))
        print(f"  {ville} {code} au niveau commune (MED_SL, statut, PR_MD60, statut) : {cas.get(code)} ; arrondissements municipaux dans Filosofi : {len(arrondissements)}")


def main(argv=None) -> int:
    parseur = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parseur.add_argument("mesure", choices=["filosofi"])
    parseur.parse_args(argv)
    filosofi()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
