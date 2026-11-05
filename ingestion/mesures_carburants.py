"""Mesures de la fiche-source carburants (docs/fiches-source/carburants.md), en lecture seule.

Rejeu, dans le conteneur airflow : python -m ingestion.mesures_carburants m1|m2|m3|m4   (ou make mesure-carburants M=m1)
  m1 : champs, types, taux de vides, ruptures (@fin renseigné ? que devient une rupture terminée ?)
  m2 : taux de changement des prix et des états, AVEC et SANS les lots de rattrapage
  m3 : code commune (quotidien) et couverture des id de l'instantané
  m4 : historique (quotidien, flux annuel de roulez-eco, en-têtes HTTP seulement)
Critère de rattrapage unique : premier passage dont l'écart avec le précédent dépasse 20 minutes.
"""
from __future__ import annotations

import argparse
import collections
import json
import re
import statistics
from datetime import date, timedelta
from urllib.parse import urljoin

import requests

from ingestion.carburants import telecharger_flux
from ingestion.db import connecter

SEUIL_RATTRAPAGE = timedelta(minutes=20)
API = "https://data.economie.gouv.fr/api/explore/v2.1/catalog/datasets/"
ROULEZ = "https://donnees.roulez-eco.fr/opendata/"
CHEMINS_ROULEZ = ("opendata/instantane", "opendata/jour", "opendata/annee", "opendata/annee/2026", "opendata/annee/2025")
EN_TETES = {"User-Agent": "observatoire-carburant"}
# Premier passage dont les états sont un rattrapage alors que l'écart entre passages est normal (1889 versions).
ETATS_RATTRAPAGE_SUPPLEMENTAIRES = ("2026-11-05 10:15:02.514229+00",)
DEBUT_JOURNAL = "2026-10-30 13:15:00+00"  # avant : passage_flux n'existe pas, les passages sans insertion sont invisibles


# --- fonctions pures ---------------------------------------------------------------------------------------


def est_vide(valeur) -> bool:
    """Absent, None, chaîne blanche, liste ou dict vide."""
    if valeur is None:
        return True
    if isinstance(valeur, str):
        return valeur.strip() == ""
    if isinstance(valeur, (list, dict)):
        return len(valeur) == 0
    return False


def taux_nulls(stations: list[dict]) -> dict:
    """Par champ : nombre de valeurs non vides, taux de vides (un champ absent d'une station compte comme vide), type."""
    n = len(stations)
    champs: dict[str, dict] = {}
    for station in stations:
        for cle, valeur in station.items():
            c = champs.setdefault(cle, {"non_vides": 0, "types": collections.Counter()})
            if not est_vide(valeur):
                c["non_vides"] += 1
                c["types"][type(valeur).__name__] += 1
    return {
        cle: {
            "non_vides": c["non_vides"],
            "taux_vide": 1 - c["non_vides"] / n if n else 0.0,
            "type": c["types"].most_common(1)[0][0] if c["types"] else "-",
        }
        for cle, c in champs.items()
    }


def lots_ecartes(horodatages, seuil=SEUIL_RATTRAPAGE) -> list[tuple]:
    """(passage, écart) du premier passage (écart None) et de tout passage séparé du précédent de plus que seuil."""
    tries = sorted(set(horodatages))
    return [(ts, tries[i] - tries[i - 1] if i else None) for i, ts in enumerate(tries) if not i or ts - tries[i - 1] > seuil]


def resume(valeurs) -> dict:
    """Effectif, moyenne, médiane et p95 (rang le plus proche) d'une liste de nombres."""
    v = sorted(valeurs)
    if not v:
        return {"n": 0, "moyenne": None, "mediane": None, "p95": None}
    return {
        "n": len(v),
        "moyenne": round(statistics.fmean(v), 1),
        "mediane": statistics.median(v),
        "p95": v[min(len(v) - 1, max(0, round(0.95 * len(v)) - 1))],
    }


def entrees_rupture(valeur) -> list[dict]:
    """Entrées de rupture d'une station ; la source les livre dans une chaîne contenant du JSON."""
    if isinstance(valeur, str):
        if valeur[:1] not in ("{", "["):
            return []
        try:
            valeur = json.loads(valeur)
        except ValueError:
            return []
    if isinstance(valeur, dict):
        valeur = valeur.get("rupture", valeur)
    if isinstance(valeur, dict):
        valeur = [valeur]
    return [e for e in valeur if isinstance(e, dict)] if isinstance(valeur, list) else []


def classer_disparitions(versions: list[tuple]) -> list[tuple]:
    """Pour une station, versions = [(collecte_ts, entrées de rupture)] triées. Renvoie (catégorie, @type) par entrée disparue.

    Une entrée est identifiée par (@id, @debut). Catégories : fin_probable ; autre_debut_plus_recent / autre_debut_plus_ancien
    (même carburant, nouvelle entrée dans la version suivante) ; reapparait_plus_tard (même carburant, autre @debut, plus tard).
    """
    sortie = []
    for i in range(len(versions) - 1):
        avant = {(e.get("@id"), e.get("@debut")): e for e in versions[i][1]}
        apres = {(e.get("@id"), e.get("@debut")): e for e in versions[i + 1][1]}
        for cle, entree in avant.items():
            if cle in apres:
                continue
            nouvelles = [e for k, e in apres.items() if k[0] == cle[0] and k not in avant]
            if nouvelles:
                plus_recent = (nouvelles[0].get("@debut") or "") >= (cle[1] or "")
                categorie = "autre_debut_plus_recent" if plus_recent else "autre_debut_plus_ancien"
            elif any(e.get("@id") == cle[0] and (e.get("@id"), e.get("@debut")) != cle for _, es in versions[i + 2 :] for e in es):
                categorie = "reapparait_plus_tard"
            else:
                categorie = "fin_probable"
            sortie.append((categorie, entree.get("@type")))
    return sortie


# --- accès base et API ----------------------------------------------------------------------------------------


def _lecture():
    conn = connecter()
    conn.set_session(readonly=True)
    return conn


def _passages(cur) -> list[tuple]:
    """(collecte_ts, lignes de prix insérées, journalisé ?, lignes lues, états changés) pour chaque passage visible."""
    cur.execute(
        """
        SELECT t.collecte_ts,
               (SELECT count(*) FROM prix_flux p WHERE p.collecte_ts = t.collecte_ts),
               j.collecte_ts IS NOT NULL, j.lignes_lues, j.etats_changes
        FROM (SELECT collecte_ts FROM prix_flux UNION SELECT collecte_ts FROM passage_flux) t
        LEFT JOIN passage_flux j USING (collecte_ts)
        ORDER BY 1
        """
    )
    return cur.fetchall()


def _afficher(titre: str, valeurs) -> None:
    r = resume(valeurs)
    print(f"  {titre:34s} n={r['n']:3d}  moyenne={r['moyenne']}  médiane={r['mediane']}  p95={r['p95']}")


# --- mesures ----------------------------------------------------------------------------------------------------


def m1() -> None:
    stations = telecharger_flux()
    print(f"## M1 champs : {len(stations)} stations, {len(taux_nulls(stations))} champs observés (vide = absent, null, '', [] ou {{}})")
    for cle, c in sorted(taux_nulls(stations).items()):
        print(f"  {cle:32s} {c['type']:6s} vide {c['taux_vide']:6.1%}")
    entrees = [e for s in stations for e in entrees_rupture(s.get("rupture"))]
    fin = sum(not est_vide(e.get("@fin")) for e in entrees)
    types = collections.Counter(e.get("@type") or "(vide)" for e in entrees)
    print(f"## M1 rupture (flux frais) : {len(entrees)} entrées, @fin renseigné {fin} ; types {dict(types)}")
    conn = _lecture()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT station_id, collecte_ts, etat->>'rupture' FROM station_etat_flux ORDER BY 1, 2")
            par_station = collections.defaultdict(list)
            for sid, ts, rupture in cur.fetchall():
                par_station[sid].append((ts, entrees_rupture(rupture)))
    finally:
        conn.close()
    classees = [c for versions in par_station.values() for c in classer_disparitions(versions)]
    n = len(classees)
    print(f"## M1 rupture (historique stocké) : {n} disparitions d'entrée")
    for categorie, k in collections.Counter(c for c, _ in classees).most_common():
        print(f"  {k:5d} ({k / n:5.1%})  {categorie}")
    print("  par @type de l'entrée disparue :", dict(collections.Counter(t or "(vide)" for _, t in classees)))


def m2() -> None:
    conn = _lecture()
    try:
        with conn.cursor() as cur:
            passages = _passages(cur)
            cur.execute("SELECT collecte_ts, count(*) FROM station_etat_flux GROUP BY 1 ORDER BY 1")
            versions = cur.fetchall()
            cur.execute("SELECT %s::timestamptz, %s::timestamptz", (DEBUT_JOURNAL, ETATS_RATTRAPAGE_SUPPLEMENTAIRES[0]))
            debut_journal, extra = cur.fetchone()
    finally:
        conn.close()
    ecartes = {ts for ts, _ in lots_ecartes([p[0] for p in passages])}
    print(f"## M2 lots écartés (écart > 20 min ou premier passage) : {len(ecartes)} sur {len(passages)} passages visibles")
    print("  PRIX insérés par passage (passages visibles ; avant le 30/10 13:02 les passages sans insertion sont invisibles)")
    _afficher("AVEC les lots de rattrapage", [p[1] for p in passages])
    _afficher("SANS les lots de rattrapage", [p[1] for p in passages if p[0] not in ecartes])
    journalises = [p for p in passages if p[2]]
    print(f"  PRIX insérés par passage, passages journalisés seulement (fiables, depuis {DEBUT_JOURNAL[:16]})")
    _afficher("AVEC", [p[1] for p in journalises])
    _afficher("SANS", [p[1] for p in journalises if p[0] not in ecartes])
    taux = [100 * p[1] / p[3] for p in journalises if p[0] not in ecartes and p[3]]
    r = resume(taux)
    print(f"  part des lignes lues réellement nouvelles, SANS rattrapage : médiane {r['mediane']:.2f} %  p95 {r['p95']:.2f} %  (n={r['n']})")
    ecartes_etats = ecartes | {extra, versions[0][0]}  # + premier chargement des états (9816 stations)
    print("  ÉTATS : versions écrites par passage ayant au moins une version (les passages sans changement sont invisibles)")
    _afficher("AVEC", [n for _, n in versions])
    _afficher("SANS (rattrapage, 1er chargement, 10:15)", [n for ts, n in versions if ts not in ecartes_etats])
    reels = [p for p in journalises if p[4] is not None]
    print("  ÉTATS changés par passage, passages journalisés avec états chargés (zéros inclus)")
    _afficher("AVEC", [p[4] for p in reels])
    _afficher("SANS (rattrapage, 1er chargement, 10:15)", [p[4] for p in reels if p[0] not in ecartes_etats])
    print("  Lots écartés pour les états, versions écrites :", [(str(ts)[5:19], n) for ts, n in versions if ts in ecartes_etats])


def m3() -> None:
    ids_instantane = {int(s["id"]) for s in telecharger_flux() if s.get("id") is not None}
    r = requests.get(
        API + "prix-carburants-quotidien/exports/json",
        params={"select": "id,com_arm_code,dep_code", "limit": -1},
        headers=EN_TETES,
        timeout=300,
    )
    r.raise_for_status()
    lignes = r.json()
    codes: dict[int, set] = collections.defaultdict(set)
    for ligne in lignes:
        if ligne.get("id") is not None:
            codes[int(ligne["id"])].add(ligne.get("com_arm_code"))
    ids_quotidien = set(codes)
    avec_code = {i for i, c in codes.items() if any(not est_vide(x) for x in c)}
    forme = re.compile(r"^(\d{5}|2[AB]\d{3})$")
    mauvais = [x for i in avec_code for x in codes[i] if not est_vide(x) and not forme.match(x)]
    commun = ids_instantane & ids_quotidien
    print(f"## M3 quotidien : {len(lignes)} lignes, {len(ids_quotidien)} id distincts ; instantané : {len(ids_instantane)} id")
    print(f"  id dans les deux : {len(commun)} ({len(commun) / len(ids_instantane):.1%} de l'instantané)")
    print(f"  instantané seulement : {len(ids_instantane - ids_quotidien)} | quotidien seulement : {len(ids_quotidien - ids_instantane)}")
    print(f"  id du quotidien avec com_arm_code renseigné : {len(avec_code)} ({len(avec_code) / len(ids_quotidien):.1%})")
    print(f"  id de l'instantané avec com_arm_code via le quotidien : {len(commun & avec_code)} ({len(commun & avec_code) / len(ids_instantane):.1%})")
    print(f"  id avec plusieurs codes différents : {sum(len({x for x in c if not est_vide(x)}) > 1 for c in codes.values())}")
    print(f"  codes de forme inattendue (hors 5 chiffres ou 2A/2B+3 chiffres) : {len(mauvais)} ; exemples {mauvais[:5]}")
    print("  concordance avec la jointure spatiale : NON MESURÉE, reportée en J6 (contours des communes)")


def m4() -> None:
    print(f"## M4 index {ROULEZ}")
    r = requests.get(ROULEZ, headers=EN_TETES, timeout=30)
    print(f"  GET index : HTTP {r.status_code} ({len(r.content)} octets) : l'index ne liste pas les fichiers")
    print("  HEAD uniquement sur les chemins connus du portail (non annoncés par l'index) : taille et date, aucun téléchargement")
    for chemin in CHEMINS_ROULEZ:
        try:
            h = requests.head(ROULEZ.rstrip("/").rsplit("/", 1)[0] + "/" + chemin, headers=EN_TETES, timeout=30, allow_redirects=True)
            print(f"  HEAD {chemin:22s} HTTP {h.status_code}  taille {h.headers.get('Content-Length')}  date {h.headers.get('Last-Modified')}  type {h.headers.get('Content-Type')}")
        except requests.RequestException as erreur:
            print(f"  HEAD {chemin:22s} erreur {type(erreur).__name__} : non vérifié")
    print("## M4 quotidien (API data.economie.gouv.fr)")
    meta = requests.get(API + "prix-carburants-quotidien", headers=EN_TETES, timeout=30).json()
    print("  modifié le", meta.get("metas", {}).get("default", {}).get("modified"))
    url = API + "prix-carburants-quotidien/records"
    limite = (date.today() - timedelta(days=7)).isoformat()
    for libelle, where, select in (
        ("lignes au total", None, "count(*) as n"),
        (f"prix_maj depuis le {limite}", f"prix_maj >= date'{limite}'", "count(*) as n"),
        (f"prix_maj avant le {limite}", f"prix_maj < date'{limite}'", "count(*) as n"),
        ("prix_maj vide", "prix_maj is null", "count(*) as n"),
        ("prix_maj le plus ancien / récent", "prix_maj is not null", "min(prix_maj) as premiere, max(prix_maj) as derniere"),
    ):
        p = requests.get(url, params={"select": select, "where": where, "limit": 1} if where else {"select": select, "limit": 1}, headers=EN_TETES, timeout=60)
        print(f"  {libelle:38s}", p.json().get("results") if p.ok else f"HTTP {p.status_code} : non vérifié")


MESURES = {"m1": m1, "m2": m2, "m3": m3, "m4": m4}


def main(argv=None) -> None:
    parseur = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parseur.add_argument("mesure", choices=sorted(MESURES))
    MESURES[parseur.parse_args(argv).mesure]()


if __name__ == "__main__":
    main()
