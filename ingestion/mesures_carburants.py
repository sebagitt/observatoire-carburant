"""Mesures de la fiche-source carburants (docs/fiches-source/carburants.md), en lecture seule.

Rejeu, dans le conteneur airflow : python -m ingestion.mesures_carburants m1|m2|m3|m4   (ou make mesure-carburants M=m1)
  m1 : champs, types, taux de vides, ruptures (@fin renseigné ? que devient une rupture terminée ?)
  m2 : taux de changement des prix et des états, AVEC et SANS les lots de rattrapage
  m3 : code commune (quotidien) et couverture des id de l'instantané
  m4 : historique (quotidien, flux annuel de roulez-eco, en-têtes HTTP seulement)
  m4b : début du flux annuel (au plus 256 Ko lus, le serveur ignore Range) : granularité des enregistrements
  m4c : unicité du zip jour de roulez-eco (état courant ou journal ?)
  m5 : latence collecte_ts moins maj, AVEC et SANS les lots écartés, minimum, régime d'été et d'hiver
  m8 : volumétrie du tampon (lignes, octets), croissance par jour hors rattrapage, poids d'un téléchargement
  m9 : couverture des com_arm_code du quotidien dans ref_cog_commune (après make charger-cog) : codes sans correspondance, arrondissements
Critère de rattrapage unique : premier passage dont l'écart avec le précédent dépasse 20 minutes.
"""
from __future__ import annotations

import argparse
import collections
import io
import json
import re
import statistics
import struct
import zipfile
import zlib
from datetime import date, timedelta
from urllib.parse import urljoin

import requests

from ingestion.carburants import FLUX_URL as FLUX_URL_M8, telecharger_flux
from ingestion.db import connecter

SEUIL_RATTRAPAGE = timedelta(minutes=20)
API = "https://data.economie.gouv.fr/api/explore/v2.1/catalog/datasets/"
ROULEZ = "https://donnees.roulez-eco.fr/opendata/"
CHEMINS_ROULEZ = ("opendata/instantane", "opendata/jour", "opendata/annee", "opendata/annee/2026", "opendata/annee/2025")
EN_TETES = {"User-Agent": "observatoire-carburant"}
# Premier passage dont les états sont un rattrapage alors que l'écart entre passages est normal (1889 versions).
ETATS_RATTRAPAGE_SUPPLEMENTAIRES = ("2026-11-05 10:15:02.514229+00",)
REGIME_HIVER = "2026-10-25 01:00:00+00"  # fin de l'heure d'été en France : 03:00 UTC+2 devient 02:00 UTC+1
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


def couverture_codes(codes, types_par_code: dict) -> dict:
    """Couverture d'une liste de codes commune (un par station, vide = sans code) face au COG.

    types_par_code : code -> ensemble des TYPECOM portant ce code. Un code est une « commune » s'il porte COM (une commune
    nouvelle et sa commune déléguée partagent un code), sinon un « arrondissement » (ARM), sinon une « deleguee_ou_associee »
    (COMD ou COMA seulement), sinon « absent » du COG.
    """
    codes = list(codes)
    renseignes = [str(c).strip() for c in codes if c is not None and str(c).strip()]

    def categorie(code: str) -> str:
        types = types_par_code.get(code, set())
        if "COM" in types:
            return "commune"
        if "ARM" in types:
            return "arrondissement"
        if types & {"COMD", "COMA"}:
            return "deleguee_ou_associee"
        return "absent"

    distincts = sorted(set(renseignes))
    modeles = ("commune", "arrondissement", "deleguee_ou_associee", "absent")
    codes_par = dict.fromkeys(modeles, 0)
    stations_par = dict.fromkeys(modeles, 0)
    for code in distincts:
        codes_par[categorie(code)] += 1
    for code in renseignes:
        stations_par[categorie(code)] += 1
    return {
        "stations": len(codes),
        "sans_code": len(codes) - len(renseignes),
        "codes_distincts": len(distincts),
        "codes_par_categorie": codes_par,
        "stations_par_categorie": stations_par,
        "codes_absents": [c for c in distincts if categorie(c) == "absent"],
    }


def m3() -> None:
    ids_instantane ={int(s["id"]) for s in telecharger_flux() if s.get("id") is not None}
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


def entete_zip(octets: bytes) -> dict:
    """En-tête local du premier membre d'un zip : nom, méthode, tailles, octets de données qui suivent."""
    signature, _v, _f, methode, _t, _d, _crc, comprimee, decompressee, n_nom, n_extra = struct.unpack("<IHHHHHIIIHH", octets[:30])
    if signature != 0x04034B50:
        raise ValueError("pas un en-tête local de zip")
    return {
        "nom": octets[30 : 30 + n_nom].decode("utf-8"),
        "methode": methode,
        "comprimee": comprimee,
        "decompressee": decompressee,
        "debut_donnees": 30 + n_nom + n_extra,
    }


def rupture_par_type(ruptures: list[str]) -> dict[str, tuple[int, int]]:
    """Par type de rupture (« (sans type) » si l'attribut manque) : (nombre de ruptures, nombre avec une fin renseignée)."""
    compte: dict[str, list[int]] = {}
    for attributs in ruptures:
        trouve = re.search(r'type="([^"]*)"', attributs)
        type_rupture = trouve.group(1) if trouve else "(sans type)"
        ligne = compte.setdefault(type_rupture, [0, 0])
        ligne[0] += 1
        ligne[1] += bool(re.search(r'fin="[^"]+"', attributs))
    return {t: (n, f) for t, (n, f) in sorted(compte.items())}


def m4b(taille_max: int = 256 * 1024) -> None:
    """Début du flux annuel : au plus 256 Ko lus (le serveur ignore Range), jamais de téléchargement complet."""
    for annee in ("2026", "2025"):
        url = f"{ROULEZ}annee/{annee}"
        with requests.get(url, headers={**EN_TETES, "Range": f"bytes=0-{taille_max - 1}"}, stream=True, timeout=60) as r:
            octets = next(r.iter_content(taille_max), b"")
            while len(octets) < taille_max:
                suite = next(r.iter_content(taille_max - len(octets)), b"")
                if not suite:
                    break
                octets += suite
            statut, contenu_range = r.status_code, r.headers.get("Content-Range")
        en_tete = entete_zip(octets)
        texte = zlib.decompressobj(-15).decompress(octets[en_tete["debut_donnees"] :]).decode("iso-8859-1")
        prix = re.findall(r'<prix nom="([^"]*)" id="\d" maj="([^"]*)" valeur="([^"]*)"', texte)
        identiques = total = 0
        for bloc in texte.split("<pdv ")[1:]:  # un bloc par station, la dernière est tronquée
            par_carburant = collections.defaultdict(list)
            for nom, _maj, valeur in re.findall(r'<prix nom="([^"]*)" id="\d" maj="([^"]*)" valeur="([^"]*)"', bloc):
                par_carburant[nom].append(valeur)
            identiques += sum(a == b for v in par_carburant.values() for a, b in zip(v, v[1:]))
            total += sum(len(v) - 1 for v in par_carburant.values())
        print(f"## M4b {annee} : HTTP {statut}, Content-Range {contenu_range} (None = Range ignoré), {len(octets)} octets lus")
        print(f"  membre {en_tete['nom']}, méthode {en_tete['methode']}, {en_tete['comprimee']} comprimés, {en_tete['decompressee']} décompressés")
        print(f"  extrait : {texte.count('<pdv ')} stations (partielles), {len(prix)} enregistrements prix, maj de {min(p[1] for p in prix)} à {max(p[1] for p in prix)}")
        print(f"  valeur identique à l'enregistrement précédent (même carburant, toutes stations de l'extrait) : {identiques}/{total}")
        ruptures = re.findall(r"<rupture ([^>]*?)/>", texte)
        avec_fin = sum(bool(re.search(r'fin="[^"]+"', r)) for r in ruptures)
        print(f"  ruptures de l'extrait : {len(ruptures)}, fin renseignée : {avec_fin} ({avec_fin / len(ruptures):.1%})")
        for type_rupture, (n, n_fin) in rupture_par_type(ruptures).items():
            print(f"    type {type_rupture} : {n} ruptures, fin renseignée {n_fin} ({n_fin / n:.1%})")


def latence_minutes(collecte_ts, maj) -> float:
    """Latence de publication d'une ligne, en minutes : notre passage moins la date de mise à jour de la source."""
    return (collecte_ts - maj).total_seconds() / 60


def m5() -> None:
    """Latence par ligne insérée (collecte_ts moins maj), AVEC et SANS les lots écartés, et latence minimale."""
    conn = _lecture()
    try:
        with conn.cursor() as cur:
            ecartes = {ts for ts, _ in lots_ecartes([p[0] for p in _passages(cur)])}
            cur.execute("SELECT collecte_ts, maj FROM prix_flux")
            lignes = cur.fetchall()
            cur.execute("SELECT %s::timestamptz", (REGIME_HIVER,))
            hiver = cur.fetchone()[0]
    finally:
        conn.close()
    avec = [(ts, latence_minutes(ts, maj), maj) for ts, maj in lignes]
    sans = [x for x in avec if x[0] not in ecartes]
    print(f"## M5 latence (minutes) par ligne de prix, {len(avec)} lignes ; lots écartés : {len(ecartes)} (critère unique > 20 min)")
    for titre, jeu in (("AVEC les lots de rattrapage", avec), ("SANS les lots de rattrapage", sans)):
        r = resume([x[1] for x in jeu])
        valeurs = [x[1] for x in jeu]
        print(f"  {titre:30s} n={r['n']}  min={min(valeurs):.1f}  médiane={r['mediane']:.1f}  p95={r['p95']:.1f}  max={max(valeurs):.1f}  négatives={sum(v < 0 for v in valeurs)}")
    for titre, jeu in (("SANS, maj en heure d'été (UTC+2)", [x for x in sans if x[2] < hiver]), ("SANS, maj en heure d'hiver (UTC+1)", [x for x in sans if x[2] >= hiver])):
        valeurs = [x[1] for x in jeu]
        if valeurs:
            r = resume(valeurs)
            print(f"  {titre:36s} n={r['n']}  min={min(valeurs):.1f}  médiane={r['mediane']:.1f}  p95={r['p95']:.1f}  négatives={sum(v < 0 for v in valeurs)}")
    print("  Notre sondage toutes les 15 min ajoute 0 à 15 min (7,5 min en moyenne) à ces latences.")


TABLES = ("prix_flux", "station_etat_flux", "passage_flux")


def m8() -> None:
    """Volumétrie du tampon : lignes et octets par table, croissance par jour hors rattrapage, poids d'un téléchargement."""
    conn = _lecture()
    try:
        with conn.cursor() as cur:
            ecartes = {ts for ts, _ in lots_ecartes([p[0] for p in _passages(cur)])}
            tailles = {}
            for table in TABLES:
                cur.execute(f"SELECT count(*), pg_total_relation_size('{table}'), pg_relation_size('{table}') FROM {table}")
                tailles[table] = cur.fetchone()
            cur.execute("SELECT date(collecte_ts AT TIME ZONE 'UTC'), collecte_ts, count(*) FROM prix_flux GROUP BY 1, 2 ORDER BY 1, 2")
            prix = cur.fetchall()
            cur.execute("SELECT collecte_ts, count(*) FROM station_etat_flux GROUP BY 1")
            etats = dict(cur.fetchall())
    finally:
        conn.close()
    print("## M8 volumétrie du tampon Postgres (table, lignes, octets avec index, octets table seule, octets par ligne)")
    for table, (n, total, seule) in tailles.items():
        print(f"  {table:20s} {n:8d}  {total:10d}  {seule:10d}  {total / n:7.0f}" if n else f"  {table:20s} 0")
    par_jour = collections.defaultdict(int)
    for jour, ts, n in prix:
        if ts not in ecartes:
            par_jour[jour] += n
    print("  prix insérés par jour UTC, lots écartés exclus (jours incomplets : 30/10 arrêt à 15:45, 05/11 en cours ; jours sans collecte absents)")
    for jour in sorted(par_jour):
        print(f"    {jour}  {par_jour[jour]:6d}")
    par_passage = collections.defaultdict(int)
    for _jour, ts, n in prix:
        if ts not in ecartes:
            par_passage[ts] += n
    octets_prix = tailles["prix_flux"][1] / tailles["prix_flux"][0]
    octets_etat = tailles["station_etat_flux"][1] / tailles["station_etat_flux"][0]
    versions = [n for ts, n in etats.items() if ts not in ecartes and ts != min(etats)]
    print("  aucun jour n'est complet dans l'historique (collecte du 23/10 au 29/10 et du 30/10 au 05/11 interrompue) : extrapolation PAR PASSAGE x 96 passages par jour")
    if par_passage:
        lignes = statistics.median(par_passage.values())
        print(f"  prix : médiane {lignes} lignes/passage hors rattrapage (n={len(par_passage)} passages avec insertion) = {lignes * 96:.0f} lignes/jour = {lignes * 96 * octets_prix / 1e6:.2f} Mo/jour ({octets_prix:.0f} octets par ligne avec index), {lignes * 96 * octets_prix * 90 / 1e6:.0f} Mo pour 90 jours")
    if versions:
        mediane = statistics.median(versions)
        print(f"  états : médiane {mediane} versions/passage ayant au moins une version (n={len(versions)}) = {mediane * 96:.0f} versions/jour = {mediane * 96 * octets_etat / 1e6:.2f} Mo/jour ({octets_etat:.0f} octets par version avec index), {mediane * 96 * octets_etat * 90 / 1e6:.0f} Mo pour 90 jours")
    with requests.get(FLUX_URL_M8, headers=EN_TETES, stream=True, timeout=120) as flux:
        recus = sum(len(morceau) for morceau in flux.raw.stream(1 << 20, decode_content=False))  # octets sur le réseau, avant décompression

        encodage = flux.headers.get("Content-Encoding")
    print(f"  téléchargement du flux instantané (1 lecture) : {recus} octets reçus sur le réseau (Content-Encoding={encodage}), JSON décompressé d'environ 29,3 Mo (29 289 015 octets mesurés le 2026-11-05)")


def m4c() -> None:
    """Unicité du zip quotidien (roulez-eco, environ 1,1 Mo) : un prix par (station, carburant) ou un journal ?"""
    r = requests.get(ROULEZ + "jour", headers=EN_TETES, timeout=120)
    r.raise_for_status()
    with zipfile.ZipFile(io.BytesIO(r.content)) as archive:
        nom = archive.namelist()[0]
        texte = archive.read(nom).decode("iso-8859-1")
    couples: dict[tuple[str, str], int] = collections.Counter()
    maj = []
    for bloc in texte.split("<pdv ")[1:]:
        station = re.match(r'id="(\d+)"', bloc).group(1)
        for carburant, date in re.findall(r'<prix nom="([^"]*)" id="\d" maj="([^"]*)"', bloc):
            couples[(station, carburant)] += 1
            maj.append(date)
    print(f"## M4c zip jour : {len(r.content)} octets, membre {nom}, {texte.count('<pdv ')} stations, {len(maj)} prix, {texte.count('<rupture ')} ruptures")
    print(f"  couples (station, carburant) distincts : {len(couples)}, avec plus d'un enregistrement : {sum(v > 1 for v in couples.values())}, maximum {max(couples.values())}")
    print(f"  maj de {min(maj)} à {max(maj)} : {'état courant (un prix par couple)' if max(couples.values()) == 1 else 'journal'}")


def m9() -> None:
    """Couverture des com_arm_code du flux quotidien dans ref_cog_commune (COG 2026) : codes sans correspondance, arrondissements."""
    r = requests.get(
        API + "prix-carburants-quotidien/exports/json",
        params={"select": "id,com_arm_code", "limit": -1},
        headers=EN_TETES,
        timeout=300,
    )
    r.raise_for_status()
    par_station: dict[int, str | None] = {}
    for ligne in r.json():
        if ligne.get("id") is None:
            continue
        code = ligne.get("com_arm_code")
        if not est_vide(code) or int(ligne["id"]) not in par_station:
            par_station[int(ligne["id"])] = None if est_vide(code) else str(code).strip()
    conn = _lecture()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT com, array_agg(DISTINCT typecom) FROM ref_cog_commune WHERE millesime = 2026 GROUP BY com")
            types_par_code = {com: set(types) for com, types in cur.fetchall()}
    finally:
        conn.close()
    res = couverture_codes(par_station.values(), types_par_code)
    print(f"## M9 couverture des com_arm_code du quotidien dans ref_cog_commune (millésime 2026, {len(types_par_code)} codes)")
    print(f"  stations : {res['stations']}, sans code : {res['sans_code']}, codes distincts : {res['codes_distincts']}")
    print(f"  codes par catégorie : {res['codes_par_categorie']}")
    print(f"  stations par catégorie : {res['stations_par_categorie']}")
    print(f"  codes sans correspondance dans le COG ({len(res['codes_absents'])}) : {res['codes_absents'][:60]}")
    codes = {c for c in par_station.values() if c}
    for ville, parent, prefixes in (("Paris", "75056", ("751",)), ("Lyon", "69123", ("6938",)), ("Marseille", "13055", ("132",))):
        arr = sorted(c for c in codes if c.startswith(prefixes))
        print(f"  {ville} : code de la commune {parent} présent dans le quotidien : {parent in codes} ; codes de forme arrondissement présents : {len(arr)} {arr[:20]}")


def couverture_filosofi(codes, valeurs: dict) -> dict:
    """Part des stations (un code par station, vide = sans code) dont la commune a MED_SL et PR_MD60 renseignés dans Filosofi.

    valeurs : codgeo (commune ou arrondissement) -> (med_sl, pr_md60), None si vide. Un code absent de `valeurs` n'a pas de ligne.
    """
    codes = list(codes)
    res = {"stations": len(codes), "sans_code": 0, "sans_ligne": 0, "med_sl": 0, "pr_md60": 0, "les_deux": 0}
    for code in codes:
        code = None if code is None or not str(code).strip() else str(code).strip()
        if code is None:
            res["sans_code"] += 1
        elif code not in valeurs:
            res["sans_ligne"] += 1
        else:
            med, pr = valeurs[code]
            res["med_sl"] += med is not None
            res["pr_md60"] += pr is not None
            res["les_deux"] += med is not None and pr is not None
    return res


def m10() -> None:
    """Part des stations du quotidien qui ont un MED_SL et un PR_MD60 renseignés (Filosofi 2023) via com_arm_code, arrondissements compris."""
    r = requests.get(API + "prix-carburants-quotidien/exports/json", params={"select": "id,com_arm_code", "limit": -1}, headers=EN_TETES, timeout=300)
    r.raise_for_status()
    par_station: dict[int, str | None] = {}
    for ligne in r.json():
        if ligne.get("id") is None:
            continue
        code = ligne.get("com_arm_code")
        if not est_vide(code) or int(ligne["id"]) not in par_station:
            par_station[int(ligne["id"])] = None if est_vide(code) else str(code).strip()
    conn = _lecture()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT codgeo, med_sl, pr_md60 FROM ref_filosofi_commune WHERE millesime = 2023")
            valeurs = {c: (m, p) for c, m, p in cur.fetchall()}
    finally:
        conn.close()
    res = couverture_filosofi(par_station.values(), valeurs)
    n = res["stations"]
    print(f"## M10 stations du quotidien et Filosofi 2023 ({len(valeurs)} zones COM et ARM), {n} stations")
    print(f"  sans code : {res['sans_code']} ; code sans ligne Filosofi : {res['sans_ligne']}")
    for cle, libelle in (("med_sl", "MED_SL renseigné"), ("pr_md60", "PR_MD60 renseigné"), ("les_deux", "les deux renseignés")):
        print(f"  {libelle} : {res[cle]} ({res[cle] / n:.1%})")


MESURES = {"m1": m1, "m2": m2, "m3": m3, "m4": m4, "m4b": m4b, "m4c": m4c, "m5": m5, "m8": m8, "m9": m9, "m10": m10}


def main(argv=None) -> None:
    parseur = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parseur.add_argument("mesure", choices=sorted(MESURES))
    MESURES[parseur.parse_args(argv).mesure]()


if __name__ == "__main__":
    main()
