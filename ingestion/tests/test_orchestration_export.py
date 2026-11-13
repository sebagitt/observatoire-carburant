"""Orchestration de l'export : schéma temporaire pour Postgres et le curseur, faux pour Databricks (aucun réseau)."""
import io
from datetime import datetime, timedelta, timezone

import pyarrow.parquet as pq
import pytest

from ingestion.db import ensure_schema
from ingestion.export_curseur import avancer_curseur, creer_table_curseur, lire_curseur
from ingestion.orchestration_export import traiter_table

UTC = timezone.utc
SOURCE = "instantane"


def t(heure, minute=0, seconde=0, jour=12):
    return datetime(2026, 11, jour, heure, minute, seconde, tzinfo=UTC)


MAINTENANT = t(15, 30)  # limite 13:30 : fenêtres 10-11, 11-12, 12-13 closes, la 13-14 ne l'est pas


@pytest.fixture
def base(conn):
    ensure_schema(conn)
    creer_table_curseur(conn)
    return conn


def prix(conn, collecte_ts, station_id):
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO prix_flux (station_id, carburant, maj, prix, collecte_ts) VALUES (%s, 'Gazole', %s, 1.5, %s)",
            (station_id, collecte_ts, collecte_ts),
        )
    conn.commit()


def etat(conn, collecte_ts, station_id):
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO station_etat_flux (station_id, collecte_ts, etat_hash, etat) VALUES (%s, %s, 'h', '{}'::jsonb)",
            (station_id, collecte_ts),
        )
    conn.commit()


def trois_fenetres(conn):
    """w1 (10-11) : 2 lignes ; w2 (11-12) : 1 ligne ; w3 (12-13) : 3 lignes."""
    prix(conn, t(10, 10), 1)
    prix(conn, t(10, 40), 2)
    prix(conn, t(11, 30), 3)
    prix(conn, t(12, 5), 4)
    prix(conn, t(12, 20), 5)
    prix(conn, t(12, 50), 6)


def compte_pg(conn, table, debut, fin):
    with conn.cursor() as cur:
        cur.execute(f"SELECT count(*) FROM {table} WHERE collecte_ts >= %s AND collecte_ts < %s", (debut, fin))
        n = cur.fetchone()[0]
    conn.rollback()
    return n


class Faux:
    """Faux Databricks : enregistre les appels dans l'ordre ; le chargement 'réussit' en retenant les fichiers reçus."""

    def __init__(self, conn, table="prix_flux", echec_envoi_a=None, echec_charger=False, destination=None, hook_compter=None):
        self.conn, self.table = conn, table
        self.appels, self.octets = [], {}
        self.echec_envoi_a, self.echec_charger = echec_envoi_a, echec_charger
        self.destination = destination or {}  # début de fenêtre -> compte imposé
        self.charges = set()
        self.hook_compter = hook_compter

    def envoyer(self, nom, octets):
        self.appels.append(("envoyer", nom))
        if self.echec_envoi_a is not None and sum(1 for a in self.appels if a[0] == "envoyer") == self.echec_envoi_a:
            raise RuntimeError("jeton-factice-123 https://hote-factice.invalid")
        self.octets[nom] = octets

    def charger(self, table, fichiers):
        self.appels.append(("charger", table, tuple(fichiers)))
        if self.echec_charger:
            raise RuntimeError("jeton-factice-123 https://hote-factice.invalid")
        self.charges.update(fichiers)

    def compter(self, table, debut, fin):
        self.appels.append(("compter", debut, fin))
        if self.hook_compter:
            self.hook_compter(debut)
        if debut in self.destination:
            return self.destination[debut]
        nom = f"{debut:%Y%m%dT%H%M%SZ}__{fin:%Y%m%dT%H%M%SZ}.parquet"
        return compte_pg(self.conn, table, debut, fin) if nom in self.charges else 0


def lancer(base, faux, table="prix_flux", **kw):
    return traiter_table(base, table, SOURCE, kw.pop("maintenant", MAINTENANT), faux.envoyer, faux.charger, faux.compter, **kw)


def genres(faux):
    return [a[0] for a in faux.appels]


# --- nominal -------------------------------------------------------------------------------------------------------


def test_nominal_sur_trois_fenetres(base):
    trois_fenetres(base)
    faux = Faux(base)
    bilan = lancer(base, faux)
    assert bilan["statut"] == "ok" and bilan["echec"] is None
    assert (bilan["fenetres_traitees"], bilan["fenetres_vides"], bilan["fenetres_verifiees"]) == (3, 0, 3)
    noms = ["prix_flux/instantane/20261112T100000Z__20261112T110000Z.parquet",
            "prix_flux/instantane/20261112T110000Z__20261112T120000Z.parquet",
            "prix_flux/instantane/20261112T120000Z__20261112T130000Z.parquet"]
    assert [a[1] for a in faux.appels if a[0] == "envoyer"] == noms
    assert bilan["fichiers_envoyes"] == [n.rsplit("/", 1)[1] for n in noms]
    assert (bilan["curseur_avant"], bilan["curseur_apres"]) == (t(10), t(13))
    assert lire_curseur(base, "prix_flux", SOURCE) == t(13)
    lignes = [pq.read_table(io.BytesIO(faux.octets[n])).num_rows for n in noms]
    assert lignes == [2, 1, 3]


def test_ordre_des_appels_envoyer_puis_charger_puis_compter(base):
    trois_fenetres(base)
    faux = Faux(base)
    lancer(base, faux)
    assert genres(faux) == ["envoyer"] * 3 + ["charger"] + ["compter"] * 3
    (appel_charger,) = [a for a in faux.appels if a[0] == "charger"]
    assert appel_charger[1] == "prix_flux" and len(appel_charger[2]) == 3


def test_etats_nominal(base):
    etat(base, t(10, 10), 1)
    etat(base, t(11, 10), 2)
    etat(base, t(11, 20), 3)
    faux = Faux(base, table="station_etat_flux")
    bilan = lancer(base, faux, table="station_etat_flux", maintenant=t(14, 30))
    assert bilan["statut"] == "ok" and bilan["fenetres_traitees"] == 2
    assert lire_curseur(base, "station_etat_flux", SOURCE) == t(12)


# --- fenêtres vides, rien à faire ------------------------------------------------------------------------------------


def test_fenetre_vide_au_milieu_sans_fichier_ni_envoi(base):
    prix(base, t(10, 10), 1)
    prix(base, t(12, 10), 2)
    faux = Faux(base)
    bilan = lancer(base, faux)
    assert (bilan["fenetres_traitees"], bilan["fenetres_vides"]) == (3, 1)
    assert len([a for a in faux.appels if a[0] == "envoyer"]) == 2
    assert all("110000Z__20261112T120000Z" not in a[1] for a in faux.appels if a[0] == "envoyer")
    compteurs = [(a[1], a[2]) for a in faux.appels if a[0] == "compter"]
    assert compteurs == [(t(10), t(11)), (t(11), t(12)), (t(12), t(13))]  # la vide est vérifiée aussi
    assert bilan["curseur_apres"] == t(13)


def test_fenetres_toutes_vides_avancent_le_curseur_sans_charger(base):
    prix(base, t(10, 10), 1)
    prix(base, t(12, 10), 2)
    faux = Faux(base)
    lancer(base, faux, max_fenetres=1)  # w1 seule : elle a une ligne
    faux2 = Faux(base)
    bilan = lancer(base, faux2, max_fenetres=1)  # w2 seule : vide
    assert "charger" not in genres(faux2) and "envoyer" not in genres(faux2)
    assert bilan["fenetres_vides"] == 1 and bilan["curseur_apres"] == t(12)


def test_aucune_fenetre_close(base):
    trois_fenetres(base)
    faux = Faux(base)
    bilan = lancer(base, faux, maintenant=t(12, 30))
    assert bilan["statut"] == "rien_a_faire" and bilan["raison"] == "aucune fenêtre close"
    assert faux.appels == []
    assert lire_curseur(base, "prix_flux", SOURCE) == t(10)  # initialisé, jamais avancé


def test_table_vide_au_depart(base):
    faux = Faux(base)
    bilan = lancer(base, faux)
    assert bilan["statut"] == "rien_a_faire" and bilan["raison"] == "table source vide"
    assert faux.appels == []
    assert lire_curseur(base, "prix_flux", SOURCE) is None


def test_max_fenetres_respecte_puis_reprise(base):
    trois_fenetres(base)
    faux = Faux(base)
    bilan = lancer(base, faux, max_fenetres=2)
    assert bilan["fenetres_traitees"] == 2 and bilan["curseur_apres"] == t(12)
    suite = lancer(base, faux, max_fenetres=2)
    assert suite["fenetres_traitees"] == 1 and suite["curseur_apres"] == t(13)


@pytest.mark.parametrize("n", [0, 1001, -1])
def test_max_fenetres_hors_bornes_refuse(base, n):
    with pytest.raises(ValueError):
        lancer(base, Faux(base), max_fenetres=n)


def test_table_hors_liste_blanche_refusee(base):
    faux = Faux(base)
    with pytest.raises(ValueError):
        traiter_table(base, "passage_flux", SOURCE, MAINTENANT, faux.envoyer, faux.charger, faux.compter)
    assert faux.appels == []


# --- échecs ---------------------------------------------------------------------------------------------------------


def test_echec_d_envoi_a_la_deuxieme_fenetre(base):
    trois_fenetres(base)
    faux = Faux(base, echec_envoi_a=2)
    bilan = lancer(base, faux)
    assert bilan["statut"] == "echec" and bilan["echec"]["phase"] == "envoi"
    assert bilan["echec"]["fenetre"] == (t(11), t(12))
    assert len([a for a in faux.appels if a[0] == "envoyer"]) == 2  # la 3e n'est pas tentée
    assert [a for a in faux.appels if a[0] == "charger"] == [("charger", "prix_flux", ("20261112T100000Z__20261112T110000Z.parquet",))]
    assert bilan["fenetres_traitees"] == 1 and len(bilan["fichiers_envoyes"]) == 1
    assert bilan["curseur_apres"] == t(11) and lire_curseur(base, "prix_flux", SOURCE) == t(11)


def test_echec_d_envoi_a_la_premiere_fenetre_ne_charge_rien(base):
    trois_fenetres(base)
    faux = Faux(base, echec_envoi_a=1)
    bilan = lancer(base, faux)
    assert bilan["echec"]["phase"] == "envoi" and "charger" not in genres(faux)
    assert bilan["curseur_apres"] == t(10)


def test_echec_de_charger_laisse_le_curseur_inchange(base):
    trois_fenetres(base)
    faux = Faux(base, echec_charger=True)
    bilan = lancer(base, faux)
    assert bilan["statut"] == "echec" and bilan["echec"]["phase"] == "chargement"
    assert "compter" not in genres(faux)
    assert bilan["curseur_avant"] == bilan["curseur_apres"] == t(10)


def test_desaccord_de_compte_a_la_deuxieme_fenetre(base):
    trois_fenetres(base)
    faux = Faux(base, destination={t(11): 0})  # la destination n'a pas la ligne de w2 (Postgres en a 1)
    bilan = lancer(base, faux)
    assert bilan["statut"] == "echec" and bilan["echec"]["phase"] == "comptage"
    assert bilan["echec"]["fenetre"] == (t(11), t(12))
    assert (bilan["echec"]["compte_postgres"], bilan["echec"]["compte_destination"]) == (1, 0)
    assert len([a for a in faux.appels if a[0] == "compter"]) == 2  # la 3e n'est pas vérifiée
    assert bilan["curseur_apres"] == t(11) and bilan["fenetres_verifiees"] == 1


def test_desaccord_a_la_premiere_fenetre_laisse_le_curseur(base):
    trois_fenetres(base)
    bilan = lancer(base, Faux(base, destination={t(10): 5}))
    assert bilan["echec"]["phase"] == "comptage" and bilan["curseur_apres"] == t(10)


def test_une_fenetre_vide_doit_etre_a_zero_a_la_destination(base):
    prix(base, t(10, 10), 1)
    prix(base, t(12, 10), 2)
    bilan = lancer(base, Faux(base, destination={t(11): 7}))
    assert bilan["echec"]["fenetre"] == (t(11), t(12))
    assert (bilan["echec"]["compte_postgres"], bilan["echec"]["compte_destination"]) == (0, 7)
    assert bilan["curseur_apres"] == t(11)


def test_erreur_de_comptage_a_la_destination_laisse_le_curseur(base):
    trois_fenetres(base)
    faux = Faux(base)

    def boom(debut):
        raise RuntimeError("jeton-factice-123")

    faux.hook_compter = boom
    bilan = lancer(base, faux)
    assert bilan["echec"]["phase"] == "comptage" and bilan["curseur_apres"] == t(10)


def test_curseur_modifie_entre_temps_est_signale(base):
    trois_fenetres(base)
    faux = Faux(base)
    deja = []

    def concurrent(debut):  # un autre processus avance le curseur pendant la phase 3
        if not deja:
            deja.append(1)
            avancer_curseur(base, "prix_flux", SOURCE, t(10), t(11))

    faux.hook_compter = concurrent
    bilan = lancer(base, faux)
    assert bilan["statut"] == "echec" and bilan["echec"]["phase"] == "curseur"
    assert lire_curseur(base, "prix_flux", SOURCE) == t(11)  # la valeur de l'autre processus est conservée


def test_rejeu_apres_echec_renvoie_les_memes_octets_et_aboutit(base):
    trois_fenetres(base)
    premier = Faux(base, echec_charger=True)
    lancer(base, premier)
    second = Faux(base)
    bilan = lancer(base, second)
    assert bilan["statut"] == "ok" and bilan["curseur_apres"] == t(13)
    assert second.octets == premier.octets  # mêmes noms, mêmes octets


def test_rejeu_apres_succes_ne_fait_rien(base):
    trois_fenetres(base)
    lancer(base, Faux(base))
    faux = Faux(base)
    bilan = lancer(base, faux)
    assert bilan["statut"] == "rien_a_faire" and faux.appels == []


def test_les_messages_ne_repetent_aucune_valeur_sensible(base):
    trois_fenetres(base)
    for faux in (Faux(base, echec_envoi_a=1), Faux(base, echec_charger=True)):
        bilan = lancer(base, faux)
        texte = repr(bilan)
        assert "jeton-factice-123" not in texte and "hote-factice" not in texte
        assert bilan["echec"]["erreur"] == "RuntimeError"


def test_echec_d_export_a_la_deuxieme_fenetre(base, monkeypatch):
    from ingestion import orchestration_export as orchestration

    trois_fenetres(base)
    reel = orchestration.exporter_fenetre
    exportees = []

    def export_qui_echoue_a_la_deuxieme(conn, table, debut, fin):
        exportees.append(debut)
        if len(exportees) == 2:
            raise RuntimeError("jeton-factice-123 https://hote-factice.invalid")
        return reel(conn, table, debut, fin)

    monkeypatch.setattr(orchestration, "exporter_fenetre", export_qui_echoue_a_la_deuxieme)
    faux = Faux(base)
    bilan = lancer(base, faux)
    assert bilan["statut"] == "echec" and bilan["echec"]["phase"] == "export"
    assert bilan["echec"]["fenetre"] == (t(11), t(12))
    assert exportees == [t(10), t(11)]  # la 3e fenêtre n'est pas exportée
    assert len([a for a in faux.appels if a[0] == "envoyer"]) == 1
    assert [a for a in faux.appels if a[0] == "charger"] == [("charger", "prix_flux", ("20261112T100000Z__20261112T110000Z.parquet",))]
    assert [a[1] for a in faux.appels if a[0] == "compter"] == [t(10)]
    assert bilan["fenetres_traitees"] == 1 and bilan["fenetres_verifiees"] == 1
    assert bilan["curseur_apres"] == t(11) and lire_curseur(base, "prix_flux", SOURCE) == t(11)
    assert "jeton-factice-123" not in repr(bilan) and bilan["echec"]["erreur"] == "RuntimeError"
