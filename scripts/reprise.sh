#!/usr/bin/env bash
# Relance la collecte (après make pause ou un arrêt de Docker) et rend un verdict.
# Ne supprime jamais rien : docker compose up -d, sans -v ni --build.
set -u
export MSYS_NO_PATHCONV=1
AIRFLOW="docker compose exec -T airflow airflow"

psql_collecte() {
  docker compose exec -T postgres sh -c "psql -U \"\$POSTGRES_USER\" -d collecte $1"
}

docker compose up -d

echo "=== attente d'Airflow (120 s max) ==="
for _ in $(seq 1 40); do
  $AIRFLOW dags list >/dev/null 2>&1 && break
  sleep 3
done

pause=$($AIRFLOW dags list 2>/dev/null | grep collecte_flux | awk -F'|' '{gsub(/ /,"",$4); print $4}')
if [ "$pause" = "True" ]; then
  echo "=== DAG en pause : unpause ==="
  $AIRFLOW dags unpause collecte_flux
  pause=$($AIRFLOW dags list 2>/dev/null | grep collecte_flux | awk -F'|' '{gsub(/ /,"",$4); print $4}')
fi
echo "is_paused : ${pause:-inconnu}"

echo "=== 5 derniers runs ==="
runs=$($AIRFLOW dags list-runs collecte_flux 2>&1)
echo "$runs" | sed -n 1,7p | cut -c1-215

echo "=== comptes ==="
psql_collecte "-c \"SELECT (SELECT count(*) FROM prix_flux) AS prix, (SELECT count(*) FROM station_etat_flux) AS etats, (SELECT count(*) FROM passage_flux) AS passages\""

echo "=== 5 dernieres lignes de passage_flux ==="
psql_collecte "-c \"SELECT * FROM passage_flux ORDER BY collecte_ts DESC LIMIT 5\""

echo "=== conteneur airflow (mémoire et PIDS) ==="
stat=$(docker stats --no-stream --format '{{.MemUsage}}|{{.PIDs}}' "$(docker compose ps -q airflow)" 2>/dev/null)
mem=${stat%%|*}
pids=${stat##*|}
gio=$(echo "${mem%% /*}" | awk '/GiB/ {sub(/GiB/,""); print $1+0; exit} /MiB/ {sub(/MiB/,""); print $1/1024; exit} {print 0}')
echo "mémoire ${mem:-inconnue}, PIDS ${pids:-inconnu}"
if awk -v g="${gio:-0}" 'BEGIN {exit !(g > 6)}'; then
  echo "AVERTISSEMENT : le conteneur airflow dépasse 6 Gio (${gio} Gio). Prévenir avant de continuer."
fi

age=$(psql_collecte "-At -c \"SELECT coalesce(round(extract(epoch FROM now() - max(collecte_ts)) / 60), 99999) FROM passage_flux\"" | tr -d '[:space:]')
dernier=$(echo "$runs" | sed -n 3p | awk -F'|' '{gsub(/ /,"",$3); print $3}')
echo "=== verdict ==="
if [ "${pause:-}" != "False" ]; then
  echo "KO : le DAG est en pause ou son état est inconnu (is_paused=${pause:-inconnu})."
elif [ "${age:-99999}" -gt 20 ]; then
  echo "EN ATTENTE : dernier passage il y a ${age} min. Le prochain quart d'heure doit apparaître : relancer make reprise ensuite."
elif [ "$dernier" = "success" ]; then
  echo "OK : collecte à jour (dernier passage il y a ${age} min, dernier run en success)."
else
  echo "ATTENTION : passage récent (${age} min) mais dernier run en '${dernier:-inconnu}' : lire le journal de la tâche."
fi
