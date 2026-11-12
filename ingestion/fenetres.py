"""Calcul des fenêtres d'export : code pur, sans base ni réseau."""

from datetime import datetime, timedelta, timezone

UNE_HEURE = timedelta(hours=1)
MARGE_PAR_DEFAUT = timedelta(hours=2)


def _exiger_fuseau(valeur: datetime, nom: str) -> None:
    """Refuse un datetime sans fuseau."""
    if valeur.tzinfo is None or valeur.utcoffset() is None:
        raise ValueError(f"{nom} doit avoir un fuseau")


def fenetres_a_exporter(
    curseur: datetime,
    maintenant: datetime,
    marge: timedelta = MARGE_PAR_DEFAUT,
) -> list[tuple[datetime, datetime]]:
    """Fenêtres d'une heure [début, fin[ consécutives à partir du curseur,
    closes quand fin <= maintenant - marge (frontière incluse)."""
    _exiger_fuseau(curseur, "curseur")
    _exiger_fuseau(maintenant, "maintenant")
    if curseur.minute or curseur.second or curseur.microsecond:
        raise ValueError("curseur doit être aligné sur l'heure")
    limite = maintenant - marge
    fenetres = []
    debut = curseur
    while debut + UNE_HEURE <= limite:
        fenetres.append((debut, debut + UNE_HEURE))
        debut += UNE_HEURE
    return fenetres


def nom_fichier(table: str, source: str, debut: datetime, fin: datetime) -> str:
    """Chemin déterministe d'un export, bornes converties en UTC."""
    _exiger_fuseau(debut, "debut")
    _exiger_fuseau(fin, "fin")
    d = debut.astimezone(timezone.utc)
    f = fin.astimezone(timezone.utc)
    return f"{table}/{source}/{d:%Y%m%dT%H%M%SZ}__{f:%Y%m%dT%H%M%SZ}.parquet"
