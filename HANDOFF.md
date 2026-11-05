# HANDOFF : fin de séance J3 en cours (2026-11-05)

## 1. Tâche suivante

J3 n'est pas terminé (périmètre allégé). À faire demain, dans cet ordre :

1. `make reprise`. Le premier verdict peut être EN ATTENTE (normal), puis relancer.
2. Mémoire d'Airflow, 30 minutes maximum : montrer un plan avant d'appliquer. Piste : limiter le parallélisme (une seule DAG, `max_active_runs=1`, trois tâches en série). Repli : `docker compose restart airflow` entre deux runs, jamais `down`.
3. `requirements-databricks.txt` épinglé (`pip freeze` sous Python 3.14.3, `databricks-sdk==0.162.0` vérifié), `make test-bootstrap` sur l'hôte, retrait du `docker compose cp` de `make test`, délai dur de 30 s sur le test de connexion.
4. Token Databricks (test d'expiration par l'utilisateur), puis `make bootstrap` deux fois de suite.
5. COG et Filosofi : fiche, suite GX, pytest, commande de rejeu, tables Postgres.
6. Fiche SIRENE sur documentation seule.
7. Clôture : debrief-jour, push, Guide, `make pause`.

Terminé quand : `make bootstrap` est rejouable sans erreur, les fiches COG, Filosofi et SIRENE sont écrites. Début de J4 : correctif de `charger` (FileNotFoundError au retry après un chargement terminé), en TDD, commit séparé, juste après la fin d'un run.

## 2. État (2026-11-05, 14:02 UTC)

- Collecte en marche (DAG `collecte_flux`, toutes les 15 min). Runs de 13:30, 13:45, 14:00 en success. `is_paused` False. `list-import-errors` vide.
- Base `collecte` : `prix_flux` 98 101, `station_etat_flux` 15 959, `passage_flux` 30 (mesuré à 14:02).
- 87 tests verts (`make test`, mesuré à 14:25). Fiche carburants committée : `docs/fiches-source/carburants.md`. M1 à M5 et M8 faites, M6 et M7 non mesurées.
- Arrêt propre à 14:37 UTC par `make pause` (conteneurs `Exited`, ni supprimés ni recréés, volume et métadonnées conservés). Dernier run avant l'arrêt : 14:30 en success. Comptes à 14:36 : `prix_flux` 98 290, `station_etat_flux` 16 044, `passage_flux` 32.
- Sauvegarde du 05/11 14:36:45 UTC : `data/backups/collecte_20261105T143645Z.dump`, 4 986 716 octets, format custom, 3 tables de données (ignorée par git).
- Dépôt poussé à 14:36 UTC (`659f938` sur origin/main).
- Airflow redémarré à 12:45:25 (`docker compose restart airflow`), preuve après le run de 13:00.
- Dépôt : branche `main`, plus de 17 commits d'avance sur origin (non poussés tant que l'utilisateur n'a pas donné son accord). `docs/journal/` et `docs/guide/` sont ignorés par git.
- Pas d'hôte 24/7 : la collecte s'arrête avec le projet (jusqu'à J15 plus 7 jours), à écrire dans le README.

### Relevés de mémoire du conteneur airflow (05/11)

| Heure UTC | Mémoire | PIDS | Processus | Zombies |
|---|---|---|---|---|
| 09:05 | 2,25 Gio | | | |
| 12:09 | 4,62 Gio | | | |
| 12:45 avant restart | 5,23 Gio | 171 | 51 | 0 |
| 12:45 après restart | 1,65 Gio | 135 | | |
| 12:51 | 1,88 Gio | 135 | 19 | 0 |
| 13:00 | 2,26 Gio | 140 | 22 | 0 |
| 13:15 | 2,19 Gio | 143 | 27 | 1 |
| 13:47 | 2,81 Gio | 150 | 31 | 0 |
| 14:02 | 3,12 Gio | 153 | 34 | 0 |
| 14:36 (avant `make pause`) | 3,46 Gio | 160 | 41 | 0 |

Hausse monotone depuis le restart (environ 20 Mio par minute), 12 workers `LocalExecutor` inactifs à 13:49. Cause non vérifiée. Seuil d'alerte : 6 Gio. Extrapolation linéaire : 6 Gio vers 16:20 à 16:30 UTC. `make reprise` affiche mémoire et PIDS avec avertissement au-delà de 6 Gio.

## 3. Décisions d'architecture

- Logique dans `ingestion/` et `quality/gx/` (pytest), DAG mince dans `airflow/dags/`. `ingestion/` et `airflow/dags/` sont montés dans le conteneur : une édition cassée arrête la collecte. Éditer juste après la fin d'un run, contrôler l'import, tester.
- Hash d'état canonique (listes triées, JSON dans les chaînes parsé). Garde-fou à 30 % de stations changées par passage. `services` reste dans le modèle (SCD2 `dim_station` prévu par la spec).
- Pas d'unicité sur (station, hash).
- `passage_flux` non exhaustif (écriture non bloquante). Couverture calculée sur l'union des `collecte_ts` de `prix_flux` et `passage_flux`.
- `com_arm_code` du quotidien : la décision de la jointure spatiale est en J6 (arrondissements de Paris, Lyon, Marseille non vérifiés face au COG).
- Flux annuel : candidat de backfill (J4 ou J5). Fuseau de `maj`, licence et complétude non vérifiés. Le XML de 359 Mo se parse en flux, jamais en mémoire dans le conteneur qui collecte.
- SIRENE : documentation seule en J3, données en J8 si B-lite.
- Databricks : catalogue `workspace`, Volume `carburant_bronze.depot`, schémas `carburant_bronze`, `carburant_silver`, `carburant_gold`. Les schémas `bronze`, `silver`, `gold`, `ci_silver`, `ci_gold` et `default` appartiennent à un AUTRE projet (EnergiScope) : jamais touchés.
- Contours des communes reportés en J6.

### Règles non négociables

- Réponses, docs et commits en français. Jamais de tirets cadratin. Aucun Co-Authored-By.
- Commits conventionnels, petits, un sujet par commit, `git add` nominatif, `git diff --cached --stat` avant. Vérifier `git config user.email` = `737222+sebagitt@users.noreply.github.com`.
- Aucun secret dans le repo ni en sortie. Ne jamais afficher `DATABRICKS_HOST` ni `DATABRICKS_TOKEN`. Ne jamais ouvrir ni toucher `.env`.
- Jamais `docker compose down -v` ni `down`. Jamais `make pause` avant la fin de séance.
- Pas de `make test` aux minutes :00, :15, :30, :45 (runs de collecte).
- Pas de push sans accord ; coller `git show --stat --oneline -3` avant.
- Fin de journée : tests verts, debrief écrit, HANDOFF à jour, commit poussé.

## 4. Schémas (base `collecte`)

```text
prix_flux(station_id BIGINT, carburant TEXT, maj TIMESTAMPTZ, prix NUMERIC(6,3), collecte_ts TIMESTAMPTZ)
  PK (station_id, carburant, maj), index sur collecte_ts, ON CONFLICT DO NOTHING

station_etat_flux(station_id BIGINT, collecte_ts TIMESTAMPTZ, etat_hash TEXT, etat JSONB)
  PK (station_id, collecte_ts)

passage_flux(collecte_ts, run_planifie, lignes_lues, lignes_inserees, etats_changes)
  une ligne par passage journalisé (non exhaustif)
```

## 5. Commandes qui marchent

```text
make up                                  docker compose up -d --build
make pause                               docker compose stop (jamais avant la fin de séance)
make reprise                             docker compose up -d, unpause si besoin, 5 runs, comptes, 5 passages, mémoire et PIDS, verdict
make test                                tests dans le conteneur (copie databricks/ avant)
make mesure-carburants M=m1              m1 à m5, m8, avec m4b et m4c
make rehash-etats [ARGS=--confirmer]     simulation par défaut, écrit seulement avec --confirmer
make collecte-run                        airflow dags test collecte_flux
```

Reprise après un arrêt : `make reprise`. Attendus : `is_paused : False`, 5 derniers runs, comptes, 5 derniers passages, mémoire sous 6 Gio, verdict. EN ATTENTE (dernier passage de plus de 20 minutes) est normal juste après la reprise : attendre le prochain quart d'heure et relancer. Après un arrêt long, le premier run peut déclencher le garde-fou des états (voir erreurs résolues).

Sur Git Bash, préfixer `MSYS_NO_PATHCONV=1` pour les chemins de conteneur. Postgres n'est pas exposé sur l'hôte : tout passe par le conteneur airflow.

## 6. Erreurs résolues

- Garde-fou 30 % déclenché à tort après la reprise du 05/11 : ordre de `services` modifié à la source. Correction : hash canonique et recalcul des hash stockés (13 460 lignes), run suivant en success.
- Fiche : "aucun champ de fin" était incomplet (`@fin` existe, jamais rempli dans l'instantané, renseigné dans le flux annuel).
- `grep -P` échoue avec la locale : vérifier les caractères en Python.
- `databricks/` non monté dans le conteneur : copie dans `make test` (à retirer demain).
- Sorties de logs longues et essais abandonnés : non conservés.

## 7. Points ouverts

- Mémoire d'Airflow : hausse monotone, cause non vérifiée (voir tableau). Plan à montrer demain.
- Cause du SIGKILL du 30/10 14:16:28 (run de 14:15) : non vérifiée.
- `charger` : FileNotFoundError au retry après un chargement terminé (début de J4).
- Métadonnées d'Airflow encore dans le conteneur : à passer sur la base Postgres `airflow`.
- `requirements-dev.txt` non épinglé ; `pandas==2.1.4` et `great-expectations==1.24.1` indisponibles sous Python 3.14. Fichier séparé `requirements-databricks.txt` prévu.
- `WorkspaceClient` avec un hôte factice bloque plus de 120 s : ne pas l'instancier dans les tests.
- Flaky `make test` : erreurs DNS occasionnelles.
- Certification Databricks à choisir (Data Analyst ou Data Engineer Associate) avant J9.
- À reporter dans le Guide : services reste dans le modèle, pas d'unicité (station, hash), couverture sur l'union, `com_arm_code`, flux annuel en backfill, SIRENE en documentation, schémas d'EnergiScope intouchables.
