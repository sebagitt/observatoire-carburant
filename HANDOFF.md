# HANDOFF : fin de séance du 2026-11-06 (J3 clos sur le critère du bootstrap)

## 1. Tâche suivante : lundi, dans cet ordre

1. `make reprise`. Premier verdict EN ATTENTE possible (normal), relancer au quart d'heure suivant. Comparer les identifiants de conteneur (section 2) avant et après : ils doivent être identiques, aucun `Recreate` dans la sortie.
2. Relecture par l'utilisateur des paragraphes de la fiche modifiés aux commits `8e3a7cd`, `3b6cde8` et `928407d` (`docs/fiches-source/carburants.md`).
3. COG et Filosofi : fiche, suite GX, pytest, commande de rejeu, tables Postgres (`ref_cog_commune`, `ref_filosofi_commune`). Licences lues sur les pages à jour avant d'être citées ; la fiche COG dit quels `TYPECOM` sont des communes (`COM`) et lesquels sont infra-communaux (`COMA`, `COMD`, `ARM`), « en vigueur » non vérifié.
4. Fiche SIRENE sur documentation seule (filtre dans un fichier de configuration, 47.11E discuté, nomenclature vérifiée, liste validée par l'utilisateur).
5. Mémoire d'Airflow : remesurer la pente sur plusieurs heures (0,103 Gio par heure sur 90 minutes, voir le tableau).
6. Début de J4 : correctif de `charger` (FileNotFoundError au retry après un chargement terminé), en TDD, commit séparé, juste après la fin d'un run.
7. Clôture : debrief-jour, push, Guide, `make pause`.

J3 est clos sur son critère : `make bootstrap` est rejouable sans erreur (deux passages, run 2 identique au run 1). Restent à faire du périmètre initial de J3 : COG, Filosofi, fiche SIRENE.

## 2. État (2026-11-06, 14:32 UTC)

- Collecte en marche (DAG `collecte_flux`, toutes les 15 minutes). Dernier run avant l'arrêt de séance : 14:30 en success. `is_paused` False.
- Tests : `make test` (conteneur) 69 passés, `make test-bootstrap` (hôte) 68 passés, soit 137 pour `make test-tout`, le critère vert de J3.
- Databricks (catalogue `workspace`) : schémas `carburant_bronze`, `carburant_silver`, `carburant_gold` et Volume `carburant_bronze.depot` créés le 06/11 vers 13:47. Les 7 schémas existants (`bronze`, `silver`, `gold`, `ci_silver`, `ci_gold`, `default`, `information_schema`) sont inchangés. Token valide jusqu'au 2027-06-17 (information de l'utilisateur).
- Airflow : `AIRFLOW__CORE__PARALLELISM=2` (commit `0695b7c`). Métadonnées d'Airflow recréées le 06/11 à 13:00 (historique des runs avant cette heure perdu).
- Pile EnergiScope : les 7 conteneurs sont passés à `restart=no` (`docker update`) par l'utilisateur et arrêtés ; son projet est terminé et publié. `make reprise` avertit si une autre pile compose tourne.
- Sauvegardes (`data/backups`, ignoré par git) : `collecte_20261106T130019Z.dump` 5 465 506 octets (avant la recréation d'airflow), `collecte_20261105T143645Z.dump` 4 986 716 octets.
- Pas d'hôte 24/7 : la collecte s'arrête avec le projet (jusqu'à J15 plus 7 jours), à écrire dans le README.

### Identifiants des conteneurs (à comparer lundi après `make pause` puis `make reprise`)

```text
observatoire-carburant-airflow-1   6bc98c619e4fae2764e5f02e87f2f69b9acc05b92793f406a37e131ac7921c76   created 2026-11-06T13:00:31Z
observatoire-carburant-postgres-1  9b3b0a0f2f3f4fa44de11f957919be837b800d6054ded613a5631df8c1805c9d   created 2026-11-06T12:20:55Z
volume                             observatoire-carburant_pg_data
```
Commande : `docker inspect observatoire-carburant-airflow-1 observatoire-carburant-postgres-1 --format "{{.Name}} {{.Id}}"`. Si un identifiant change, un conteneur a été recréé : c'est un incident (métadonnées d'Airflow perdues, DAG en pause).

### Relevés de mémoire du conteneur airflow

Avant `parallelism=2` (05/11, 32 workers possibles) : 1,65 Gio à 12:45 après restart, 3,46 Gio à 14:36 (environ 0,95 Gio par heure), 5,23 Gio avant ce restart.

Après `parallelism=2` (06/11) :

| Heure UTC | Mémoire | PIDS | Workers (corrigé) | Zombies |
|---|---|---|---|---|
| 13:01 | 2,088 Gio | 137 | 1 | 0 |
| 13:31 | 2,179 Gio | 139 | 2 | 0 |
| 13:48 | 2,089 Gio | 139 | 2 | 0 |
| 14:01 | 2,223 Gio | 139 | 2 | 0 |
| 14:31 | 2,248 Gio | 140 | 2 | 0 |

Critère provisoire : au moins 5 relevés sur 90 minutes ou plus (atteint), 2 workers au plus (atteint), alerte au-dessus de 3,5 Gio (non franchie, maximum 2,248 Gio), pente sous 0,1 Gio par heure : **0,103 Gio par heure, juste au-dessus, non atteint de peu**. Mesure fragile (5 points, 90 minutes). Workers corrigés : retirer 1 aux comptages qui incluaient le script de relevé. Un worker pèse environ 200 Mio de RSS (199 et 200 Mio mesurés). Cause de la pente résiduelle non cherchée.

## 3. Décisions d'architecture

- Logique dans `ingestion/` et `quality/gx/` (pytest), DAG mince dans `airflow/dags/`. `ingestion/` et `airflow/dags/` sont montés dans le conteneur : une édition cassée arrête la collecte. Éditer juste après la fin d'un run, contrôler l'import, tester.
- Hash d'état canonique (listes triées, JSON dans les chaînes parsé). Garde-fou à 30 % de stations changées par passage. `services` reste dans le modèle (SCD2 `dim_station` prévu par la spec).
- Pas d'unicité sur (station, hash).
- `passage_flux` non exhaustif (écriture non bloquante). Couverture calculée sur l'union des `collecte_ts` de `prix_flux` et `passage_flux`.
- `com_arm_code` du quotidien : la décision de la jointure spatiale est en J6 (arrondissements de Paris, Lyon, Marseille non vérifiés face au COG).
- Flux annuel : candidat de backfill (J4 ou J5). Fuseau de `maj`, licence et complétude non vérifiés. Le XML de 359 Mo se parse en flux, jamais en mémoire dans le conteneur qui collecte.
- SIRENE : documentation seule en J3, données en J8 si B-lite.
- Contours des communes reportés en J6.
- Databricks : catalogue `workspace`, Volume `carburant_bronze.depot`, schémas `carburant_*`. Les schémas `bronze`, `silver`, `gold`, `ci_silver`, `ci_gold` et `default` appartiennent à un AUTRE projet (EnergiScope) : jamais touchés (test sur les noms qualifiés).
- Bootstrap : trois contrôles distincts (variables nommées, authentification dans un processus enfant tué après 30 s, entrepôt prêt par `SELECT 1` scruté au plus 5 fois), au plus 5 interrogations par requête, arrêt à la première erreur, aucune valeur sensible affichée. Un entrepôt serverless arrêté démarre à la première requête (documenté, l'état PENDING signifie « en attente de l'entrepôt »).
- Les quatre variables `DATABRICKS_HOST`, `DATABRICKS_TOKEN`, `DATABRICKS_CATALOG`, `DATABRICKS_WAREHOUSE_ID` sont lues dans l'environnement du processus (noms dans `.env.example`, valeurs dans `.env`). Pour `make bootstrap` et `make bootstrap-controles` seulement, make inclut `.env`.

### Règles non négociables

- **`make reprise` ne recrée jamais un conteneur** : `docker compose up -d --no-recreate`, jamais un `up -d` simple après un `pause`. Exception : `--no-recreate` n'applique pas un changement de configuration ; il faut alors un `up -d` simple (ou `up -d --no-deps airflow`) après un `pg_dump` frais, en connaissant la perte des métadonnées d'Airflow. Preuve par les identifiants de conteneur avant et après.
- Ne jamais lire ni afficher `.env`. Ne jamais afficher `DATABRICKS_HOST`, `DATABRICKS_TOKEN` ni l'identifiant d'entrepôt.
- Réponses, docs et commits en français. Jamais de tirets cadratin. Aucun Co-Authored-By.
- Commits conventionnels, petits, un sujet par commit, `git add` nominatif, `git diff --cached --stat` avant. Vérifier `git config user.email` = `737222+sebagitt@users.noreply.github.com`.
- Aucun secret dans le repo ni en sortie.
- Jamais `docker compose down -v` ni `down`. Jamais `make pause` avant la fin de séance.
- Pas de `make test` aux minutes :00, :15, :30, :45 (runs de collecte).
- Pas de push sans accord ; coller `git show --stat --oneline -3` avant.
- Fin de journée : tests verts (`make test-tout`), debrief écrit, HANDOFF à jour, commit poussé.

## 4. Schémas

Base `collecte` (Postgres) :

```text
prix_flux(station_id BIGINT, carburant TEXT, maj TIMESTAMPTZ, prix NUMERIC(6,3), collecte_ts TIMESTAMPTZ)
  PK (station_id, carburant, maj), index sur collecte_ts, ON CONFLICT DO NOTHING

station_etat_flux(station_id BIGINT, collecte_ts TIMESTAMPTZ, etat_hash TEXT, etat JSONB)
  PK (station_id, collecte_ts)

passage_flux(collecte_ts, run_planifie, lignes_lues, lignes_inserees, etats_changes)
  une ligne par passage journalisé (non exhaustif)
```

Databricks (catalogue `workspace`) : `carburant_bronze` (avec le Volume `depot`), `carburant_silver`, `carburant_gold`. Tables de référence prévues lundi : `ref_cog_commune` (clé `TYPECOM`, `COM`), `ref_filosofi_commune` (clé `codgeo`, `millesime`).

## 5. Commandes qui marchent

```text
make up                                  docker compose up -d --build
make pause                               docker compose stop (jamais avant la fin de séance)
make reprise                             up -d --no-recreate, unpause si besoin, 5 runs, comptes, 5 passages, autres piles, mémoire et PIDS, verdict
make test                                tests du conteneur (69)
make test-bootstrap                      tests du bootstrap Databricks sur l'hôte, dans .venv (68)
make test-tout                           conteneur et hôte : critère vert de J3
make mesure-carburants M=m1              m1 à m5, m8, avec m4b et m4c
make rehash-etats [ARGS=--confirmer]     simulation par défaut, écrit seulement avec --confirmer
make collecte-run                        airflow dags test collecte_flux
make bootstrap-controles                 variables, authentification, entrepôt prêt (aucune écriture)
make bootstrap                           les trois contrôles, puis le SQL deux fois avec SHOW SCHEMAS avant / après le run 1 / après le run 2
```

Venv de l'hôte : `python -m venv .venv` puis `.venv/Scripts/pip install -r requirements-databricks.txt` (Python 3.14.3, versions épinglées).

Reprise après un arrêt : `make reprise` (fonctionne sous PowerShell et Git Bash). Attendus : conteneurs `Running` sans `Recreate`, `is_paused : False`, 5 derniers runs, comptes, 5 derniers passages, « aucune autre pile compose en cours », mémoire sous 6 Gio, verdict. EN ATTENTE (dernier passage de plus de 20 minutes) est normal juste après la reprise : attendre le prochain quart d'heure et relancer. Après un arrêt long, le premier run peut déclencher le garde-fou des états. `KO : base injoignable` signifie que le dernier passage n'a pas pu être lu.

Sur Git Bash, préfixer `MSYS_NO_PATHCONV=1` pour les chemins de conteneur. Postgres n'est pas exposé sur l'hôte : tout passe par un conteneur.

## 6. Erreurs résolues

- **Recréation involontaire le 2026-11-06 12:20:55 UTC** : un `make reprise` (avec `up -d` simple) lancé depuis PowerShell a détruit puis recréé `postgres` et `airflow`. Données conservées (volume `pg_data`), métadonnées d'Airflow perdues, run de 12:15 rejoué (passage en double à 12:21:19, 0 ligne). Cause non établie : hash de configuration identique dans les deux shells, aucun `env_file`, seules `POSTGRES_USER` et `POSTGRES_PASSWORD` sont lues dans `.env`, donc l'ajout de variables `DATABRICKS_*` ne l'explique pas. Correctif : `--no-recreate`.
- `make reprise` sous PowerShell : `bash` du PATH est le lanceur WSL ; le Makefile utilise le bash de Git for Windows déduit de `git --exec-path`.
- `KO authentification` en une seconde au premier essai : `TypeError` de mon code (`WorkspaceClient` n'accepte pas `http_timeout_seconds`, attribut de `Config`). Corrigé en TDD.
- Mémoire d'Airflow : le `LocalExecutor` crée un worker par tâche jusqu'à `parallelism` (32) et ne les arrête jamais. Plafonné à 2.
- Garde-fou 30 % déclenché à tort après la reprise du 05/11 : ordre de `services` modifié à la source. Correction : hash canonique et recalcul des hash stockés (13 460 lignes), run suivant en success.
- Fiche : « aucun champ de fin » était incomplet (`@fin` existe, jamais rempli dans l'instantané, renseigné dans le flux annuel).
- Un test collecté par erreur par pytest car la fonction s'appelait `tester_...` : renommée `controler_...`.
- `grep -P` échoue avec la locale : vérifier les caractères en Python.
- Sorties de logs longues et essais abandonnés : non conservés.

## 7. Points ouverts

- Pente de mémoire d'Airflow : 0,103 Gio par heure avec `parallelism=2`, à remesurer sur plusieurs heures ; cause résiduelle non cherchée.
- Cause de la recréation de 12:20 : non établie.
- Cause du SIGKILL du 30/10 14:16:28 (run de 14:15) : non vérifiée (la mémoire du conteneur partagée avec l'autre pile est une piste).
- Passage à zéro insertion du 29/10 09:45 cité dans une ancienne version de la fiche : non vérifiable, journaux perdus (voir `928407d`). Les journaux de tâche sont dans le conteneur et sont perdus à chaque recréation.
- `charger` : FileNotFoundError au retry après un chargement terminé (début de J4).
- Métadonnées d'Airflow encore dans le conteneur : à passer sur la base Postgres `airflow`.
- `requirements-dev.txt` non épinglé ; `pandas==2.1.4` et `great-expectations==1.24.1` indisponibles sous Python 3.14 (d'où `requirements-databricks.txt` séparé).
- Durée d'un démarrage à froid de l'entrepôt serverless : non mesurée (environ une minute au premier contrôle).
- Flaky `make test` : erreurs DNS occasionnelles.
- Certification Databricks à choisir (Data Analyst ou Data Engineer Associate) avant J9.
- À reporter dans le Guide : `services` reste dans le modèle, pas d'unicité (station, hash), couverture sur l'union des `collecte_ts`, `com_arm_code` (décision en J6), flux annuel en backfill (J4 ou J5), SIRENE en documentation seule, schémas d'EnergiScope intouchables, règle `--no-recreate` et son exception, `parallelism=2`, `make test-tout`.
