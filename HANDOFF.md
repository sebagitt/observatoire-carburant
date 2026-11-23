# HANDOFF : fin de séance du 2026-11-23, dernière mesure à 15:59 UTC (J7 et J8 clos : error_code, dim_station, ruptures typées, fct_ruptures, contracts et garde des accès)
## 0. Mise à jour du 2026-11-23, J8 (à lire en premier)

Spécifications : `docs/journal/J7_spec.md` (section 12b : D, E, C) et `docs/journal/J8_spec.md` (section 9b). Debrief : `docs/journal/J8.md`. Mesures du STOP 0 de J8 : `docs/journal/J8_stop0/`. Guide : `docs/journal/guides/J8_guide.md`. Toutes les heures de ce fichier sont en UTC.

J7 et J8 sont clos. Prochaine séance : section 1.

### Reprise du 23/11
- `chargement_bronze` des 20, 21 et 22/11 en success (celui du 22/11 rejoué le 23/11 de 07:52 à 07:54), puis run du 23/11 12:20 en success (12:20:00 à 12:20:48). `warehouses.start` accepté par `make lire-bronze` à 09:40 ; aucun refus depuis le 19/11.

### J7 terminé
- Section D (commit `29adcfd`) : `ErreurBootstrap("démarrage de l'entrepôt impossible (<classe>, <error_code ou inconnu>)")`, jamais le message du SDK, `from None` gardé, `error_code` filtré sur `[A-Z][A-Z0-9_]{0,63}`. 4 tests ; MD1 et MD2 rouges. Le diagnostic est visible dans le log d'Airflow, pas dans `make charger-bronze` (section 7).
- Section E (commit `1aef54c`) : skill `dbt-conventions`, exception SCD2 depuis un journal de versions.
- Section C (commit `0e84217`) : `dim_station` en îlots, 5 unit tests, 6 tests de données (test singulier avec `group='carburants'` pour lire le staging `private`) ; MS1 à MS3 rouges. M4 du 23/11 12:36 : 11 498 lignes, 9 855 stations ; table relue égale.

### J8
- STOP 0 : 40 596 événements de rupture, 8 936 stations, 0 changement de type. `debut` étiqueté `+00:00` mais en heure de Paris : 5 268 débuts après leur première observation lus en UTC, 0 lus en heure de Paris (J8-M4c). Passage à l'heure d'hiver 2026 : **25/10 01:00 UTC** (pas le 26/10). Heures ambiguës ou inexistantes dans les données : 0.
- Section 5 (commit `cd6933d`) : 12 colonnes de rupture typées dans le staging ; unit test de fuseau à 4 lignes ; filet D3b (0.17, 0.28, 0.07, 0.37, 0.38, 0.23) et `accepted_values` (`definitive`, `temporaire`).
- Section 6 (commit `f0df100`) : `int_ruptures__observations` (vue, 105 222 lignes) et `fct_ruptures` (table, 40 596 événements, 22 789 en cours, 95 sans commune) ; 5 unit tests ; tests singuliers `rupture_debut_ts <= premiere_observation_ts` et `fin_observee_ts >= premiere_observation_ts` (error), un type par événement (warn) ; MR1 et MR2 rouges.
- Section 7 (commit `38a7dd8`) : les trois marts en `public`, contract appliqué, `not_null` sur 9 clés (`is_nullable` NO relu) ; MK (`station_id` déclaré `string`) arrêté à la compilation. `marts/` reste `protected` par défaut dans `dbt_project.yml` : chaque mart se déclare `public`.
- Section 8 (commit `8d86f3f`) : garde, `marts/` implique `public` et contract, `public` implique `marts/` ; G1 et G2 rouges.

### Tests et commits
- `make test-tout` (15:58:57 UTC, code retour 0) : 462 + 73 + 5 = **540**.
- Commits du 23/11 : `29adcfd`, `1aef54c`, `0e84217`, `cd6933d`, `f0df100`, `38a7dd8`, `8d86f3f`, plus le commit de ce HANDOFF.
- État du dépôt : `origin/main` à `6abe30b` au début de la séance ; push prévu juste après ce commit, à vérifier par `git ls-remote` à la reprise.

## 0a. Archive du 2026-11-19, J7 partiel

Spécification : `docs/journal/J7_spec.md` (D3b et méthode de mutation tranchées en section 8). Debrief : `docs/journal/J7.md`. Mesures du STOP 0 : `docs/journal/J7_stop0/`. Toutes les heures de ce fichier sont en UTC.

### Prochaine séance (faite le 23/11, voir bloc 0), dans l'ordre
1. Correction de l'`error_code` dans `assurer_entrepot` (`ingestion/adaptateurs_databricks.py`) : garder le type de l'exception et son `error_code` dans `ErreurBootstrap`, jamais le message ; TDD ; écriture dans `ingestion/` en fenêtre :01 à :14, hors run en cours.
2. Section C de `docs/journal/J7_spec.md` : `dim_station` (attendu de lignes recalculé par M4 juste avant le build ; 11 479 au 19/11 16:06).

### Reprise du 19/11
- Conteneurs démarrés à 08:19:25 (airflow) et 08:19:20 (postgres), identifiants égaux à la section 2 (lus à 10:19). `origin/main` égal à `HEAD` (`c2c2b8e`) : push de J6 confirmé.
- Premier run de `collecte_flux` (`scheduled__2026-11-17T14:45`, 08:19:34 à 08:19:53) en success : 1 034 stations changées sur 9 852 connues, **10,50 %**, garde-fou des 30 % non déclenché (après environ 42 h d'arrêt) ; 1 036 versions (2 stations nouvelles).
- `chargement_bronze` : rejeu `scheduled__2026-11-18T12:20` (tentatives à 08:19:50 et 08:24:54) et run du 19/11 12:20 (12:25:06) en `failed`, `controler` en `upstream_failed` ; `ErreurBootstrap: démarrage de l'entrepôt impossible` (`warehouses.start` refusé, entrepôt lu `STOPPED`). `start` de diagnostic accepté à 15:32:11, `RUNNING` à 15:32:20. Cause des refus : inconnue (section 7).
- `make dbt ARGS="parse"` ne démarre pas l'entrepôt (`STOPPED` à 14:11:47 et à 14:15:01, parse à 14:14:34).

### Rattrapage du bronze (manuel, `--max-appels 1`)
- `make charger-bronze ARGS="--table prix_flux --max-fenetres 48 --max-appels 1"` (16:02:39, 63 s) puis `--table station_etat_flux` (16:04:10, 52 s) : 48 fenêtres chacune [17/11 10:00, 19/11 10:00[, 41 vides, 7 fichiers.
- Totaux relus à 16:05:12 : `prix_flux` **233 649**, `station_etat_flux` **31 799** (attendus : 212 301 + 21 348 et 30 154 + 1 645), doublons 0, max `collecte_ts` 2026-11-19 09:45:01 ; curseurs `export_curseur` au 2026-11-19 10:00.
- Freshness à 16:05:27 : 3 PASS (`prix_flux` et `station_etat_flux` 6,34 h, `ref_station_commune` 88,09 h).
- Avant le rattrapage, le dernier `collecte_ts` du bronze était 2026-11-17 09:45:02 (et non « vers 10:45 » comme écrit le 17/11).
- Mémoire d'airflow : 2,263 Gio (15:40), 2,35 Gio (16:02 et 16:03), 2,264 Gio (16:07) ; 140 PIDS.

### Section A (commit `1cb0bb8`)
- Entrée `unit_test` retirée de `TYPES_GARDES`. M5 avant le retrait : 0 nœud `unit_test` dans `manifest["nodes"]`, 13 sous `manifest["unit_tests"]` ; macros `test_not_null_proportion` et `test_mutually_exclusive_ranges` présentes (dbt_utils 1.4.1). Garde 4 passed à 16:34 et 17:31.

### STOP 0 (détail dans `docs/journal/J7.md`)
- M0 : 18 colonnes extraites par `get_json_object`. M1 : 31 799 lignes, taux de 54,1181 % à 100 %. M2 : 33 clés, toutes présentes sur toutes les lignes ; compte M1 égal aux valeurs non nulles de M2 pour les 18 colonnes. M3 : `services_json` tableau JSON de chaînes (D6 confirmée). M4 : 9 854 stations, `dim_station` attendue à 11 479 lignes ; 2 533 versions dues au seul ordre de `services`.
- 12 clés de rupture pour J8 : `e10`, `e85`, `gazole`, `gplc`, `sp95`, `sp98`, chacune `_rupture_debut` et `_rupture_type`.

### Section B (commit `aba7a07`)
- 18 tests `dbt_utils.not_null_proportion` en error sur `stg_carburants__station_etats` : D3 (taux moins 1 point, au millième inférieur) 0.99 ou 0.988 sur 11 colonnes ; D3b (moitié du taux, au centième inférieur) sur `horaires_json` 0.43, `services_json` 0.46, `ruptures_json` 0.48, `carburants_disponibles_json` 0.45, `carburants_indisponibles_json` 0.45, `carburants_rupture_definitive_liste` 0.39, `carburants_rupture_temporaire_liste` 0.27. Description de `code_departement` réécrite (34 lignes au 19/11).
- `make dbt ARGS="build --select stg_carburants__station_etats"` (16:30:43 à 16:31:35) : PASS 32, WARN 1 (`not_null_stg_carburants__station_etats_code_departement`, 34 lignes), ERROR 0 ; 29 tests de données (11 avant), 3 unit tests.
- Mutations sans écriture (montage du SQL compilé du test sur le modèle muté, `dbt show`) : témoin 0 ligne (latitude et ville), MN1 `$.latitud` et MN2 `$.vile` 1 ligne chacune, `not_null_proportion` 0.0 ; empreinte du modèle réel identique avant et après.

### Tests et commits
- `make test-tout` (17:31:07 UTC, code retour 0) : 458 + 73 + 4 = **535** (aucun test pytest ajouté).
- Commits du 19/11 : `1cb0bb8` test(dbt) retrait de l'entrée unit_test de la garde, `aba7a07` test(dbt) filet de taux de non-NULL, plus le commit de ce HANDOFF.
- État du dépôt : `origin/main` à `c2c2b8e` au début de la séance ; push prévu juste après ce commit, à vérifier par `git ls-remote` à la reprise.

## 0b. Archive du 2026-11-17, J6

Spécification : `docs/journal/J6_spec.md` (section 12 : écarts 5 à 9 validés). Debrief : `docs/journal/J6.md`. Mesures du STOP 0 : `docs/journal/J6_stop0/`. Toutes les heures de ce fichier sont en UTC.

### Reprise du jeudi 19/11 : faite, voir bloc 0

### Référentiels au bronze
- Rattachement station commune : `com_arm_code` du flux quotidien (option D, décision du 17/11). `make charger-station-commune` (10:02 UTC) : fichier de 6 492 958 octets, `fichier_date` 2026-11-16, table Postgres `ref_station_commune` à 9 952 lignes dont 9 941 avec code, validée par GX (minimum 9 500, format du code, station unique) et remplacée en une transaction.
- `make charger-referentiels-bronze ARGS="--table toutes"` (lancé à 10:47:12 UTC, 44 s) : 5 tables dans `carburant_bronze`, comptes égaux à Postgres relus avant (`ref_cog_commune` 37 496, `ref_cog_departement` 101, `ref_cog_region` 18, `ref_filosofi_commune` 34 813, `ref_station_commune` 9 952). Une table est remplacée en entier : un Parquet par table et par chargement dans `/Volumes/workspace/carburant_bronze/depot/referentiels/<table>/`, puis `CREATE OR REPLACE TABLE ... AS SELECT * FROM read_files(...)`, puis comptage. Garde : seulement `workspace.carburant_bronze.<table de la liste>` et le dossier `referentiels/` du Volume.
- `read_files` ajoute **`_rescued_data`** (string) : gardée (écart 8), 0 valeur non nulle sur les 5 tables ; test `expression_is_true "_rescued_data is null"` en error sur chaque source. Schéma en section 4.
- Mémoire d'airflow pendant les chargements : 2,35 à 2,45 Gio.

### dbt (J6)
- Sources : `insee` (schéma `carburant_bronze`, 4 tables COG et Filosofi, `freshness: null`) ; `ref_station_commune` ajoutée à `carburant_bronze`, freshness sur `cast(fichier_date as timestamp)`, warn 45 jours, error 90 jours (écart 7 : un vieux fichier rechargé ne paraît pas frais).
- Variables : `cog_millesime: 2026`, `filosofi_millesime: 2023`.
- Staging (vues privées, `carburant_silver`) : `stg_insee__cog_communes` (clé `commune_cle` sur millesime, typecom, com ; 37 496), `stg_insee__cog_departements` (101), `stg_insee__cog_regions` (18), `stg_insee__filosofi_communes` (clé `filosofi_cle` sur geo_object, codgeo ; 34 813), `stg_carburants__station_communes` (9 952). `charge_ts` est l'heure du bronze partout (même sens que `stg_carburants__prix`), `postgres_charge_ts` l'heure de la base de collecte.
- `carburant_gold.dim_commune` (table, `protected`, sans contract jusqu'à J8) : une ligne par COM ou ARM, **34 920** (34 875 COM et 45 ARM, attendu recalculé sur le staging) ; parent : `comparent` pour un ARM, le code lui-même pour un COM ; Filosofi joint sur (`geo_object = commune_type`, `codgeo = commune_code`), même géographie que le COG 2026 (fiche filosofi, ligne 11) ; taux de `niveau_vie_median_eur` renseigné 88,31 %, test singulier en warn sous 87,31 %.
- `carburant_silver.int_stations__commune` (vue, `protected`) : une ligne par station de `stg_carburants__station_etats` (9 851). Code COM ou ARM tel quel, sinon parent d'un COMD ou d'un COMA (écart 6) ; lit `stg_insee__cog_communes` (écart 9) ; `departement_code_flux` du dernier état, `departement_code_cog` du COG ; `rattachement_source` = `flux_quotidien` si un code est trouvé.
- **Couverture : 9 829 sur 9 851, 99,777 %**, égale au STOP 0 (18 stations absentes du quotidien, 4 sans code, 0 code hors COG) ; **seuil 99,27 %**, test singulier en error. 0 écart de département (test en warn). 9 817 stations couvertes ont un niveau de vie médian.
- Build `+dim_commune +int_stations__commune` (13:35:42 à 13:37:20 UTC) : PASS 91, WARN 1, ERROR 0. Le warn : `not_null_stg_carburants__station_etats_code_departement`, 31 lignes pour 4 stations (24 lignes au J5).
- Garde des schémas : 4 tests ; nouveau test par dossier (`marts/` dans `carburant_gold`, `intermediate/` dans `carburant_silver`), sondes sans config sous `marts/` et `intermediate/`.
- Mutations (copies hors dépôt, restauration prouvée par empreinte) : filtre de millésime, MT, MP, MD, MC, MG tuées ; **MI** (`and direct.commune_code is null` retiré) survit : mutant équivalent (le `coalesce` préfère déjà le COM, aucun code à plusieurs lignes COMD ou COMA dans le COG 2026), code inchangé (décision du 17/11).
- **Le connecteur SQL de dbt démarre l'entrepôt de lui-même** : `STOPPED` à 08:06:57 UTC, `RUNNING` à 08:07:45 après `dbt source freshness` (une seule observation). Contraire à `execute_statement` du 13/11, refusé sur un entrepôt arrêté (bloc 0e).

### Heures des rapports
- Les heures du matin du 17/11 étaient déjà en UTC (preuves : freshness `2026-11-17T08:07:34+00:00` ; Parquet `ref_cog_region__20261117T100326Z` écrit entre 10:03:16 et 10:03:41). Seul le premier rapport de l'après-midi était en heure locale (13:25 à 13:28 au lieu de 12:25 à 12:28 UTC) ; la correction qui avait converti 10:47 en 09:47 était fausse. Voir `learnings.md`.

### Tests et commits
- `make test-tout` (14:31:54 UTC, code retour 0) : conteneur 458, hôte 73, garde dbt 4, soit **535** (attendu recalculé : 534 plus le nouveau test de garde).
- Commits du 17/11 : `77b7009` docs (règles d'accord groupé), `6f4cdb9` feat(ingestion) rattachement station commune, `472fe45` feat(ingestion) référentiels vers le bronze, `28dec48` build cibles make, `11df76a` feat(dbt) sources et staging Insee, `8518325` feat(dbt) staging du rattachement, `03e5825` feat(dbt) dim_commune, `0739187` feat(dbt) rattachement des stations, `a65211e` test(dbt) garde intermediate et marts, `65cdc14` docs(fiches) flux quotidien et com_arm_code, plus le commit de ce HANDOFF.
- État du dépôt : `origin/main` à `2f3f4da` au début de la séance ; push prévu juste après ce commit, à vérifier par `git ls-remote` à la reprise.

## 0c. Archive du 2026-11-16, ancien J5 dbt
Archive : le bloc 0 fait foi. Correction du 17/11 : les commits listés ci-dessous comme « non poussés à l'écriture » ont été poussés ensuite, `origin/main` à `2f3f4da` (`git ls-remote` du 17/11 à la reprise).

Spécification : `docs/journal/J5_spec_dbt.md` (section 9 : écarts validés au STOP D). Debrief : `docs/journal/J5_dbt.md`.

### Reprise de session (dans cet ordre)
1. `make reprise`, puis comparer les identifiants des conteneurs avec la section 2 (inchangés le 16/11 à 15:36 : airflow `f12be32bd7bc...`, postgres `9b3b0a0f...`).
2. `git status -sb`, `git log origin/main..HEAD --oneline`, `git ls-remote origin refs/heads/main` : relever l'état réel avant toute consigne qui cite des étapes déjà faites.
3. **Avant le premier dbt de la séance** : relever l'état de l'entrepôt en lecture seule (SDK, `warehouses.get`, champ `state` uniquement, jamais l'identifiant) ; sert à trancher si le connecteur SQL de dbt démarre l'entrepôt seul (7 s au `dbt debug` du 16/11, cause non établie).
4. Ne jamais lire `.env`, ne jamais afficher `DATABRICKS_*` ni l'identifiant d'entrepôt.

### Projet dbt (`dbt/`, projet `observatoire_carburant`)
- dbt Core **1.12.6** (contrainte `dbt-core<1.12.7` de dbt-databricks **1.13.0**), dans `.venv-dbt` (Python 3.12.10, `requirements-dbt.txt`, 84 paquets). dbt_utils 1.4.1 (`packages.yml`, `package-lock.yml`). `dbt --version` annonce dbt v2 : on reste en 1.12.6 (spec), pas de 1.12.7 ni de v2.
- Profil `dbt/profiles.yml` versionné, uniquement des `env_var` ; `make dbt` inclut `.env` pour cette seule cible et pose `DBT_PROFILES_DIR` et `DBT_PROJECT_DIR` sur `dbt/`. Cible `dev`, catalogue `workspace`, `threads: 2`.
- Macro `generate_schema_name` : schéma personnalisé tel quel. Staging en vue dans `carburant_silver`, `+group: carburants` sur tout le projet, `+access: private` sur staging. Tests rattachés à `carburant_silver` (`data_tests: +schema`).
- Sources `carburant_bronze.prix_flux` et `station_etat_flux` : freshness sous `config` sur `collecte_ts`, **warn 28 h, error 52 h** (calcul : marge de 2 h de `ingestion/fenetres.py`, donc `max(collecte_ts)` vers J 09:45 après le run de 12:20 ; âge maximal normal 26 h 36, 27 h 01 avec timeout et retry ; deux chargements manqués 51 h 01). Un error le lundi après un arrêt du week-end est attendu. `dbt source freshness` en pass le 16/11 à 15:03 (âge 4,31 h).
- **Deux vues dans `workspace.carburant_silver`** (seules relations du schéma, `information_schema` du 16/11) : `stg_carburants__prix` (197 932 lignes, clés distinctes 197 932) et `stg_carburants__station_etats` (28 260 lignes, clés distinctes 28 260), égales au bronze relu.
- `dbt build --select staging` : PASS=38, WARN=1 (24 lignes sans `code_departement`), ERROR=0, TOTAL=39. Unit tests : maj sans conversion de fuseau (ligne du 29/03, passage à l'heure d'été), coordonnées divisées par 100 000 (entière, décimale, non numérique), `automate_24_24` en comparaison exacte, clé absente à NULL. Mutations G1 (division par 10 000) et G2 (`from_utc_timestamp`) vues rouges.
- Garde des schémas `make test-dbt-garde` (`dbt/garde/test_garde_schemas.py`, 3 tests) : `dbt parse` dans pytest avec variables factices (`.env` jamais lu), manifest temporaire ; schémas `carburant_*` pour tous les nœuds et sources, staging privé, sonde de schémas exacts. Incluse dans `make test-tout`. Elle a trouvé la fuite `dbt_test__audit` (voir `learnings.md`).
- **`dbt/logs/` et `dbt/target/` ne sont jamais affichés en brut ni collés** : `dbt.log` contient l'hôte et le `http_path` en clair. Recherche ciblée seulement, avec l'hôte, `http_path` et l'identifiant d'entrepôt masqués.
- `dbt show --inline` passé par make : écrire `$$.` dans les chemins JSON, requête dans un fichier (heredoc à délimiteur entre guillemets) ; pas de `limit` dans la requête.
- Le flux instantané ne contient **aucun code INSEE** (voir section 3).

### À faire ensuite
1. Push des commits de la séance après accord (contrôles de la section 1).
2. J6 : code commune des stations (aucun code INSEE dans le flux instantané) : jointure spatiale (lat/lon et contours), `cp` plus `ville` vers le COG (ambigu), `com_arm_code` du flux quotidien ; à trancher en J6. Référentiels COG et Filosofi.
3. J7 : filet de taux de NULL sur les colonnes extraites du JSON (`get_json_object` renvoie NULL sans erreur sur une clé mal écrite) ; 12 colonnes de rupture typées ; snapshot `dim_station`.
4. Garde : l'entrée `unit_test` de `TYPES_GARDES` est inerte (les unit tests sont sous `manifest["unit_tests"]`, et ne créent qu'une vue temporaire de session) : à retirer ou à documenter.
5. Points ouverts hors J5 : voir le bloc 0c.

### Tests et commits
- `make test-tout` (15:35 UTC, code retour 0) : conteneur 400, hôte 73, garde dbt 3, soit **476** passés.
- `origin/main` à `9febba3` (`git ls-remote`, 15:36 UTC). Commits locaux de la séance, non poussés à l'écriture : `6e87cee`, `abf7ce1`, `50a3729`, `545618a`, `3763a66`, `465a9e1`, `2089c2d`, plus le commit de ce HANDOFF.

## 0d. Archive du 2026-11-16, fin du 3e jour de J5
Archive : le bloc 0 fait foi.

### Reprise de session (dans cet ordre)
1. `make reprise`, puis comparer les identifiants des conteneurs avec ceux de la section 2 (airflow `f12be32bd7bc...`, postgres `9b3b0a0f...`). Après un arrêt, le dernier déclenchement manqué de chaque DAG est rejoué une fois (voir ci-dessous) ; si `charger` de la collecte échoue avec le garde-fou des 30 %, voir `learnings.md` (12/11).
2. `git status -sb` et `git log origin/main..HEAD --oneline` : relever l'état réel avant toute consigne qui cite des étapes déjà faites.
3. Ne jamais lire `.env`, ne jamais afficher `DATABRICKS_*` ni un mot de passe.

### Chargement vers le bronze (Databricks)
- DAG `chargement_bronze` **actif** : `20 12 * * *` UTC, `catchup=False`, `max_active_runs=1`, retries 1 (5 min), timeout 20 min. Deux tâches : `charger` (`assurer_entrepot`, puis `charger_bronze.main`, toutes les tables, 48 fenêtres, 3 appels ; échec si un statut n'est ni `ok` ni `rien_a_faire`) puis `controler` (lecture `lire_bronze` des deux tables, échec sans retry si doublons > 0). Logique dans `ingestion/taches_bronze.py`.
- `catchup=False` avec `CronTriggerTimetable` : le dernier déclenchement passé est lancé une fois à la reprise ou à l'activation (observé le 16/11 : collecte 12:45 et 13:00 rejoués, `chargement_bronze` de 12:20 lancé à 13:06). Rejeux sans effet (idempotence).
- Premier run (déclenchement de 12:20, lancé à 13:06:58) : `success`, charger 30,9 s (entrepôt prêt en 11,1 s), controler 4,7 s ; une fenêtre par table ; curseurs des deux tables au **2026-11-16 11:00 UTC**.
- Totaux Databricks (lus à 13:08 par `make lire-bronze`) : `prix_flux` **197 932**, `station_etat_flux` **28 260**, doublons **0** (attendus recalculés : 196 832 + 1 100 et 28 060 + 200).
- Rattrapage manuel du 16/11 avant le DAG : `station_etat_flux` du 29/10 13:00 au 16/11 10:00 (9 appels, 431 s), `prix_flux` du 13/11 12:00 au 16/11 10:00 (2 appels, 76 s) ; contrôle global T0 moins D0 plus P vérifié (D0 = 168 lignes du 09/11 10:00 déjà chargées).
- `make lire-bronze` : lecture seule (total, min, max, doublons de clé, lignes d'une plage `[debut, fin[`), démarre l'entrepôt si besoin, aucune écriture.

### Incident du 16/11 07:21 (clos)
- Deux runs de `collecte_flux` (scheduled__2026-11-15T12:30 et scheduled__2026-11-16T07:15) en échec sur `valider` : flux reçu à 2 859 stations, sous `LIGNES_MIN = 5 000` des états ; `charger` en `upstream_failed` ; run de 07:30 normal. Cause dans la source : inconnue (réponse HTTP non conservée). Le contrôle a joué son rôle.

### Bascule d'airflow (16/11)
- Métadonnées d'Airflow sur la base Postgres `airflow` (neuve : ancienne base sauvegardée, puis `DROP` et `CREATE`). `databricks-sdk==0.156.0` dans l'image (`airflow/requirements.txt`). Interface sur `127.0.0.1:8081`.
- Les quatre `DATABRICKS_*` sont passées par nom dans `environment:` du compose (`NOM: ${NOM}`, valeurs lues dans `.env` par compose) : **révise la décision `AIRFLOW_CONN_DATABRICKS`**. Le jeton est visible par `docker inspect` et dans l'environnement des tâches.
- Mot de passe de l'interface régénéré à chaque recréation : `/opt/airflow/simple_auth_manager_passwords.json.generated` dans le conteneur, à lire soi-même.
- Procédure de recréation en trois temps : `docker compose stop airflow`, attendre que le port 8081 soit libre (`Get-NetTCPConnection -LocalPort 8081`), `docker compose up -d --no-deps airflow` ; si `ports are not available` : attendre 30 s puis `docker compose start airflow` (sans recréer). Premier essai du 16/11 en échec sur le port (cause inconnue), retour arrière, second essai réussi.
- Après une recréation : `collecte_flux` et `chargement_bronze` sont créés en pause (`dags_are_paused_at_creation=True`) : `airflow dags unpause` pour chacun.
- Sauvegardes prises juste avant : `data/backups/airflow_20261116T124713Z.dump` (112 103 octets) et `data/backups/collecte_20261116T124713Z.dump` (10 138 178 octets), lisibles par `pg_restore --list`.
- `make test` utilise une base de métadonnées sqlite dédiée (`/tmp/airflow_tests.db` du conteneur) : comptes de la base `airflow` identiques avant et après les tests.

### Périmètre
- Retirés du périmètre le 16/11 : B-lite (SIRENE), petits changements de `charger_bronze` (noms de fichiers, durée de l'entrepôt). Objectif 3 retiré.
- Retard d'environ 3 jours : le tampon J16-J17 est absorbé.

### À faire ensuite
1. Ancien J5 : `dbt init`, sources avec freshness, staging carburants (spécification donnée par le chat, ne pas deviner).
   - J6 : code commune des stations à obtenir (le flux instantané n'a aucun code INSEE, voir section 3) : jointure spatiale, `cp` plus `ville` vers le COG (ambigu) ou `com_arm_code` du flux quotidien, à trancher en J6.
2. Points ouverts hors J5 :
   - journaliser `observed_value` dans `valider` (aujourd'hui seul le type d'expectation est journalisé) ;
   - minimum GX de 5 000 lignes d'états (environ 51 % du volume), faible priorité : une station absente du flux n'est jamais écrite ;
   - brancher la purge (`data/lots` grossit) ;
   - refonte du garde-fou des 30 % (comparaison au lot précédent) ;
   - Makefile : `test_lire_bronze.py` et les tests Databricks (`test_chargement_databricks.py`, `test_adaptateurs_databricks.py`, `test_charger_bronze.py`, `test_taches_bronze.py`) aussi sur l'hôte ;
   - `passage_flux.etats_changes` exclut les stations nouvelles (définition à connaître : l'écart de 1 à 07:30 le 16/11 est une station nouvelle).
3. Clôture de séance : debrief-jour, `make test-tout`, HANDOFF, push après accord.

### Tests et commits
- `make test-tout` (13:31 UTC, code retour 0) : conteneur 400 passés, hôte 73 passés.
- `origin/main` à `b4f1a1a` à l'écriture (13:31 UTC) ; ces commits et celui du HANDOFF ont été poussés ensuite : `origin/main` à `9febba3` (`git ls-remote` du 16/11 à 13:57 UTC, aucun commit en avance).

## 0e. Archive du 2026-11-13, fin du 2e jour de J5
Archive : lignes périmées (curseurs, totaux, à faire) ; le bloc 0 fait foi.

### Reprise de session (dans cet ordre)
1. `make reprise`, puis comparer les identifiants des conteneurs avec ceux de la section 2 (airflow `6bc98c61...`, postgres `9b3b0a0f...`). Après un arrêt, un rattrapage est attendu ; si `charger` échoue avec le garde-fou des 30 %, voir l'incident du 12/11 dans le bloc 0c.
2. `git status -sb` et `git log origin/main..HEAD --oneline` : relever l'état réel avant toute consigne qui cite des étapes déjà faites.
3. Reprendre le rattrapage de `station_etat_flux` (ci-dessous), puis les petits changements de `charger_bronze`, puis le DAG.
4. Ne jamais lire `.env`, ne jamais afficher `DATABRICKS_*`.

### État du chargement vers Databricks (carburant_bronze)
- Chaîne prouvée en réel : export Parquet par fenêtre, envoi dans le Volume, `COPY INTO` avec liste exacte de fichiers, comptage de contrôle, curseur dans la table `export_curseur` (base `collecte`).
- `prix_flux` : rattrapage terminé jusqu'au 13/11 12:00 UTC (dernière fenêtre close), 173 874 lignes des deux côtés, 42 fichiers distincts. La fenêtre du 09/11 de 10:00 à 11:00, chargée à la main le 13/11, est rejouée sans doublon.
- `station_etat_flux` : curseur au 29/10 13:00 UTC, plage égale à Postgres (10 003 lignes), total Databricks 10 171 (les 168 lignes de la fenêtre du 09/11 sont au-delà du curseur). Reste environ 35 fenêtres non vides et 15 400 lignes (mesure du 13/11 à 15:25, à recalculer par `COUNT` Postgres) : lots 4 (9 fenêtres), 7, 8, 9 et 11.
- Curseurs actuels dans `export_curseur` : prix_flux au 2026-11-13 12:00 UTC (mis à jour 14:21 UTC), station_etat_flux au 2026-10-29 13:00 UTC (mis à jour 14:24 UTC).
- Commande : `make charger-bronze ARGS="--table station_etat_flux --max-fenetres 48 --max-appels 3"`. Précaution de lancement (pas une règle technique) : entre :02 et :08 d'un quart d'heure pour un lot qui contient des données, entre :02 et :11 pour un lot vide.
- Mesures : démarrage à froid de l'entrepôt 10,6 s et 11,0 s (deux observations) ; environ 0,9 à 1,5 s par fenêtre vide ; une fenêtre de 25 067 lignes passe sans problème ; mémoire d'airflow stable à 2,3 Gio pendant les lots.

### Constats Databricks
- `execute_statement` sur un entrepôt arrêté est refusé (`BAD_REQUEST`) : il faut `warehouses.start` puis attendre `RUNNING` (`assurer_entrepot` le fait). Cause : hypothèse, non établie.
- Littéral `TIMESTAMP '...+00:00'` accepté, fuseau traité (806 lignes avec des bornes en heure de Paris converties).
- `COPY INTO` : `_metadata.file_path` dans le `SELECT` et `FILES` avec `FROM (SELECT ...)` fonctionnent ; les noms de `FILES` sont relatifs au dossier source ; `_fichier_source` porte le préfixe `dbfs:`.
- Un fichier déjà chargé est ignoré par `COPY INTO` (documentation lue) : la comparaison des comptes de plage est le garde-fou contre une divergence silencieuse.
- Le Parquet d'une même fenêtre est identique à l'octet (vérifié à plusieurs heures d'intervalle).

### Méthode de lancement réel
- `make charger-bronze` : inclut `.env` pour cette seule cible, passe les quatre `DATABRICKS_*` par nom au conteneur jetable (aucune valeur dans la commande), installe `databricks-sdk==0.156.0` dans ce conteneur (environ 3 s) avec les contraintes d'Airflow 3.0.6, puis lance `python -m ingestion.charger_bronze`. Aucune valeur n'est affichée.
- `databricks-sdk` 0.162.0 exige `certifi>=2026.10.19`, alors que les contraintes d'Airflow 3.0.6 imposent 2025.8.3 : seule la 0.156.0 passe.
- Environnement Python de l'image : `sys.prefix` vaut `/home/airflow/.local` ; `pip install --user` y est refusé.
- Règle de contrôle : avant chaque lot, lire le total de la table et calculer l'attendu à partir du total mesuré plus le `COUNT` Postgres de la plage. Ne jamais recopier un attendu (erreur du 13/11 : les 33 180 lignes du 23/10 avaient été oubliées).
- Scripts de lancement hors dépôt dans un dossier scratch local, chemin non versionné. Le dépôt est la source de vérité : ne pas en dépendre.

### À faire ensuite
1. Finir `station_etat_flux` (ci-dessus).
2. Petits changements de `charger_bronze` : afficher les noms de fichiers envoyés (troncature au-delà de 10 noms) et la durée rendue par `assurer_entrepot` (objet de suivi optionnel). Tests et mutations décrits dans le rapport de CC du 13/11.
3. Optimisation du comptage seulement si le coût le justifie : une requête par plage avec `GROUP BY date_trunc('hour', collecte_ts)`.
4. DAG de chargement (imports du SDK dans les fonctions, `catchup=False`, `max_active_runs=1`) qui démarre l'entrepôt avec `assurer_entrepot` avant tout SQL, puis la recréation unique d'airflow (`pg_dump` frais, SDK dans l'image, base de métadonnées neuve, connexion `AIRFLOW_CONN_DATABRICKS`, port 8081 sur 127.0.0.1), accord à redemander.
5. Makefile : ajouter `test_chargement_databricks.py`, `test_adaptateurs_databricks.py` et `test_charger_bronze.py` aux tests de l'hôte (commit `build:` séparé).
6. À décider : index sur `station_etat_flux.collecte_ts` si l'export ralentit.
7. Mettre à jour les sections 1 et 2 de ce fichier (tête de `origin/main`, nombre de tests, identifiants, mémoire).

### Tests et commits
- `make test-tout` : conteneur 326 passés et 2 ignorés, hôte 73 passés (15:28, code retour 0).
- Commits de J5 poussés le 2026-11-13 jusqu'à b4f1a1a :
  - `b4f1a1a` build: cible make charger-bronze et script de lancement
  - `a5aaa05` feat(ingestion): commande de chargement du bronze
  - `fa99799` feat(ingestion): adaptateurs Databricks pour l'orchestration
  - `05b3531` feat(ingestion): orchestration de l'export par fenêtres
  - `4323122` docs(handoff): fin du 1er jour de J5
  - `df7149b` feat(ingestion): couche Databricks du chargement (SQL, chemins, envoi)
  - `68f2c0e` feat(ingestion): export d'une fenêtre en Parquet mémoire
  - `7a8ec03` feat(ingestion): table et fonctions de curseur d'export
  - `aae57b0` feat(ingestion): fenêtres d'export à bornes semi-ouvertes
  - `e2ac6da` build: databricks-sdk 0.156.0 (contraintes d'Airflow 3.0.6)
  - `4de9de5` docs(handoff): fin de J4

## 1. Prochaine séance et reprises sans séance

### Prochaine séance (après J8)
1. Contrôles habituels de reprise (points 1 à 4 ci-dessous), puis `git ls-remote origin refs/heads/main` : push du HANDOFF de J8 à vérifier.
2. STOP 0 de `docs/journal/J9_spec.md` (lecture seule) ; rien n'est écrit avant accord.

### Reprises sans séance

But : laisser la collecte tourner par périodes, sans aucune écriture à la main.

À chaque reprise, dans l'ordre :
1. `make reprise` ; identifiants des conteneurs égaux à la section 2 ; verdict EN ATTENTE normal au début, relancer au quart d'heure suivant.
2. `docker compose exec -T airflow airflow dags list-runs collecte_flux` : premier run en success. S'il échoue sur `charger` (garde-fou des 30 %) : ne rien faire, noter, prochaine séance.
3. `docker compose exec -T airflow airflow dags list-runs chargement_bronze` : le rejeu du dernier 12:20 part juste après la reprise ; noter success ou failed. Si failed : ne rien relancer.
4. `docker stats --no-stream observatoire-carburant-airflow-1` : mémoire sous 3,5 Gio.
5. En fin de journée : `make pause` à une minute hors :00, :15, :30, :45.

Règles :
- Si possible, pile en route à 12:20 UTC (13:20 à Paris) : seconde chance pour le chargement si le rejeu échoue.
- Aucune écriture à la main : ni `make charger-bronze`, ni `make dbt`, ni `tasks clear`. Un run de chargement reprend jusqu'à 3 appels de 48 fenêtres (déduit du code, non observé au-delà de 48).
- Pas plus de 48 h sans reprise (garde-fou bloqué après 69 h le 12/11, passé après 42 h le 19/11 à 10,5 %).
- Freshness en ERROR après une pause : attendue.
- Les trous de collecte retardent `debut_validite_ts` de `dim_station` (D8) : à écrire dans le guide et le README.

### Contrôles avant tout push (repris de l'ancienne section 1, point 7 ; CLAUDE.md y renvoie)

`git status -sb`, `git log origin/main..HEAD --oneline`, `git diff origin/main..HEAD --stat`, noms de fichiers sans `.env`, `data/`, `.venv`, `docs/journal`, `docs/guide`, `.dump`, `.zip`, aucun Co-Authored-By, chemin local, secret ni tiret cadratin, et `git show --stat --oneline -3` collé.

### Jours clos

J8 est clos : 12 colonnes de rupture typées (heure de Paris convertie), `int_ruptures__observations`, `fct_ruptures`, contracts et accès `public` sur les trois marts, garde des accès ; `make test-tout` 540.

J7 est clos : garde nettoyée, filet de taux de non-NULL, `error_code` dans l'échec de démarrage de l'entrepôt, skill SCD2, `dim_station` historisée.

J6 est clos : référentiels au bronze, staging Insee, `dim_commune`, `int_stations__commune` ; couverture 99,777 % au-dessus du seuil de 99,27 % ; `make test-tout` 535.

J3 est clos : `make bootstrap` rejouable (deux passages, run 2 identique au run 1), COG et Filosofi chargés dans `collecte` avec GX, SIRENE en documentation seule (données en J8 si B-lite).

J4 est clos sur le correctif de `charger` (`4fa7b1f`), la purge confirmée par le contenu (écrite et testée, non branchée), les mutations de la purge et quatre tests ajoutés (`848b170`, `6f281bb`, `83b498a`, `05494b7`), et le test de transmission du jeton (`f35c605`). Le DAG de chargement vers Databricks n'est pas écrit : le critère initial de J4 n'est pas atteint, il est reporté en J5 par décision de l'utilisateur.

## 2. État (2026-11-17, dernière mesure : identifiants des conteneurs et mémoire à 14:33 UTC)

- Collecte en marche (DAG `collecte_flux`, toutes les 15 minutes, `catchup=False`) ; runs de 14:00, 14:15 et 14:30 UTC le 17/11 en `success`. `chargement_bronze` actif (12:20 UTC chaque jour), run du 17/11 de 12:20 en `success` (12:20:00 à 12:21:31).
- Tests : `make test` (conteneur) 458, `make test-bootstrap` (hôte) 73, `make test-dbt-garde` (hôte, `.venv-dbt`) 4, soit **535** pour `make test-tout` (17/11 14:31 UTC, code retour 0).
- Dépôt : `origin/main` à `6abe30b` au début de la séance du 23/11 ; commits du 23/11 listés au bloc 0 ; push prévu juste après ce commit, à vérifier par `git ls-remote` à la reprise.
- Mise à jour du 23/11 : tests 540 (15:58 UTC) ; `carburant_gold` : tables `dim_commune` (34 920), `dim_station` (11 498), `fct_ruptures` (40 596) ; `carburant_silver` : vue `int_ruptures__observations` en plus ; détail au bloc 0.
- Mise à jour du 19/11 : tests 535 (17:31 UTC), bronze à 233 649 et 31 799 lignes (16:05 UTC), curseurs au 2026-11-19 10:00, détail au bloc 0a ; le reste de cette section date du 17/11.
- dbt (23/11) : `carburant_silver` : 7 vues de staging, les vues `int_stations__commune` et `int_ruptures__observations` ; `carburant_gold` : les tables `dim_commune`, `dim_station` et `fct_ruptures`, publiques sous contract. Détail au bloc 0.
- Bronze : `prix_flux`, `station_etat_flux` et les 5 tables `ref_*` (SHOW TABLES du 17/11 à 10:48 UTC). SHOW SCHEMAS : 10 noms, inchangés le 17/11 (relus à 13:10 et 13:17 UTC).
- Lots Parquet : `charger` ne supprime plus ses fichiers (logique dans `ingestion/lots.py`). `purger_lots` (âge supérieur à 48 h ET prix par clé et états par station et hash retrouvés dans Postgres, confirmation écrite dans `purge_journal.jsonl`) **n'est branchée nulle part**. `data/lots` grossit (16:31 : 78 fichiers, 193 168 Kio ; les 48 fichiers du 09/11 pèsent 137 590 639 octets ; extrapolation, non mesurée sur 24 h : environ 550 Mo par jour). Les 20 lots d'avant le 09/11 (12 retrouvés par contenu dont 7 lots du 05/11 dont les états avaient été bloqués par le garde-fou, 8 du 23/10 avec une `maj` décalée de 2 h) ne sont ni rechargés ni purgés (décision de l'utilisateur).
- Tables de référence dans la base `collecte` : `ref_cog_commune` 37 496 lignes (millésime 2026), `ref_cog_departement` 101, `ref_cog_region` 18, `ref_filosofi_commune` 34 813 lignes (millésime 2023, 34 768 communes plus 45 arrondissements). Clé de `ref_filosofi_commune` : (`millesime`, `geo_object`, `codgeo`).
- Databricks (catalogue `workspace`) : schémas `carburant_bronze`, `carburant_silver`, `carburant_gold` et Volume `carburant_bronze.depot` créés le 06/11 ; dossier `referentiels/` du Volume (un Parquet par table et par chargement, les anciens restent : `ref_cog_region` en a deux). Les 7 schémas existants (`bronze`, `silver`, `gold`, `ci_silver`, `ci_gold`, `default`, `information_schema`) sont inchangés. Token valide jusqu'au 2027-06-17 (information de l'utilisateur).
- Airflow : `AIRFLOW__CORE__PARALLELISM=2` (commit `0695b7c`). Métadonnées sur la base Postgres `airflow` depuis le 16/11 13:05 UTC (historique des runs antérieur perdu, ancienne base sauvegardée).
- Pile EnergiScope : projet terminé et publié, ses 7 conteneurs sont à `restart=no` et arrêtés. `make reprise` avertit si une autre pile compose tourne.
- Sauvegardes (`data/backups`, ignoré par git) : `collecte_20261116T124713Z.dump` et `airflow_20261116T124713Z.dump` (avant la bascule du 16/11) ; `collecte_20261108T145530Z.dump` (6 974 655 octets, UTC), pris à l'arrêt propre du 08/11 ; `collecte_20261106T130019Z.dump` (avant la recréation d'airflow) ; `collecte_20261105T143645Z.dump`. Faire un `pg_dump` frais avant toute recréation.
- Pas d'hôte 24/7 : la collecte s'arrête avec le projet (jusqu'à J15 plus 7 jours), à écrire dans le README.
- Fiches : `docs/fiches-source/carburants.md` (section J6 du 17/11 : flux quotidien et `com_arm_code`), `cog.md`, `filosofi.md`, `sirene.md`.
- Journal de leçons : `docs/journal/learnings.md` (ignoré par git, 31 entrées au 17/11, cause étiquetée établie, hypothèse ou inconnue). Détail des jours : `docs/journal/J6.md`, `docs/journal/J5_dbt.md`, `docs/journal/J3.md`, `docs/journal/J4.md` et `docs/journal/J4_mutations_purge.md` (ignorés).
- Règles de pédagogie : section « Pédagogie et leçons » de `CLAUDE.md` (expliquer en une ou deux lignes les commandes qui modifient un état, entrée de trois lignes dans `learnings.md` après chaque correction comprise, recouper les messages qui citent des étapes déjà faites, aucun commit de `docs/fiches-source` sans accord).

### Identifiants des conteneurs (à comparer après `make pause` puis `make reprise`)

```text
observatoire-carburant-airflow-1   f12be32bd7bc75c3bc9ad08fc431a64e10600bc603c85bf94ebe92f5c80dac62   created 2026-11-16T13:05:37Z   image 3e865283013c
observatoire-carburant-postgres-1  9b3b0a0f2f3f4fa44de11f957919be837b800d6054ded613a5631df8c1805c9d   created 2026-11-06T12:20:55Z
volume                             observatoire-carburant_pg_data
```
airflow recréé le 16/11 (bascule, procédure du bloc 0d) ; postgres inchangé depuis le 06/11. Commande : `docker inspect observatoire-carburant-airflow-1 observatoire-carburant-postgres-1 --format "{{.Name}} {{.Id}}"`. Si un identifiant change, un conteneur a été recréé : incident (métadonnées d'Airflow perdues, DAG en pause).

### Relevés de mémoire du conteneur airflow

Avant `parallelism=2` (05/11, 32 workers possibles) : 1,65 Gio à 12:45 après restart, 3,46 Gio à 14:36 (environ 0,95 Gio par heure), 5,23 Gio avant ce restart.

Après `parallelism=2` (workers corrigés : le script de relevé n'est pas compté) :

| Heure UTC | Mémoire | PIDS | Workers | Zombies |
|---|---|---|---|---|
| 06/11 13:01 | 2,088 Gio | 137 | 1 | 0 |
| 06/11 13:31 | 2,179 Gio | 139 | 2 | 0 |
| 06/11 13:48 | 2,089 Gio | 139 | 2 | 0 |
| 06/11 14:01 | 2,223 Gio | 139 | 2 | 0 |
| 06/11 14:31 | 2,248 Gio | 140 | 2 | 0 |
| 08/11 12:35 | 1,696 Gio | 135 | 0 | 0 |
| 08/11 13:05 | 2,110 Gio | 138 | 2 | 0 |
| 08/11 13:35 | 2,022 Gio | 139 | 2 | 0 |
| 08/11 14:05 | 2,082 Gio | 140 | 2 | 0 |
| 08/11 14:27 | 2,262 Gio | 140 | non recompté | non recompté |
| 16/11 12:37 | 2,321 Gio | non relevé | non relevé | non relevé |
| 16/11 13:31 | 2,424 Gio (conteneur recréé à 13:05) | non relevé | non relevé | non relevé |
| 16/11 13:56 | 2,288 Gio | 143 | non relevé | non relevé |
| 16/11 15:36 | 2,422 Gio | 142 | non relevé | non relevé |
| 17/11 10:02 | 2,35 Gio avant, 2,45 Gio après charger-station-commune | non relevé | non relevé | non relevé |
| 17/11 10:47 | 2,42 Gio (avant charger-referentiels-bronze) | non relevé | non relevé | non relevé |
| 17/11 10:48 | 2,453 Gio (après) | non relevé | non relevé | non relevé |
| 17/11 14:33 | 2,527 Gio | 141 | non relevé | non relevé |

Critère provisoire : au moins 5 relevés sur 90 minutes ou plus, 2 workers au plus, pente sous 0,1 Gio par heure, alerte au-dessus de 3,5 Gio (jamais franchie, maximum 2,262 Gio). Pente (moindres carrés) : **0,103 Gio par heure le 06/11 et 0,103 le 08/11** (13:05 à 14:27, 4 relevés, 82 minutes ; 0,233 avec le relevé de 12:35 pris avant que les workers existent, point biaisé) : le seuil de 0,1 n'est pas tenu, de très peu. Un worker pèse environ 200 Mio de RSS. Cause de la pente résiduelle : hypothèse, non cherchée.

## 3. Décisions d'architecture

- Logique dans `ingestion/` et `quality/gx/` (pytest), DAG mince dans `airflow/dags/`. `ingestion/` et `airflow/dags/` sont montés dans le conteneur : une édition cassée arrête la collecte. `ingestion/` hors `tests/` (dont `lots.py`) et `airflow/dags/` : écriture en fenêtre :01 à :14 de chaque quart d'heure, puis hors run en cours (`airflow dags list-runs collecte_flux`, aucun `running`) ; contrôler l'import, tester. `ingestion/tests/` : écriture hors :00, :15, :30, :45 et hors run en cours.
- Hash d'état canonique (listes triées, JSON dans les chaînes parsé). Garde-fou à 30 % de stations changées par passage : **il fonctionne** (le 05/11 il a bloqué un lot parce que la source avait réordonné `services`, l'empreinte l'ignore maintenant). `services` reste dans le modèle (SCD2 `dim_station` prévu par la spec).
- Pas d'unicité sur (station, hash) : une station peut revenir à un état déjà vu.
- `passage_flux` non exhaustif (écriture non bloquante). Couverture calculée sur l'union des `collecte_ts` de `prix_flux` et `passage_flux`.
- **Le flux instantané (JSON `etat` de `station_etat_flux`) ne contient aucun code INSEE** (mesure du 16/11 par `dbt show` : 33 clés, aucune de code commune ; seulement `cp`, `ville`, `code_departement`). **Tranché le 17/11 : `com_arm_code` du flux quotidien (option D)**, pas de jointure spatiale.
- `com_arm_code` du quotidien : **Paris, Lyon et Marseille sont codées par arrondissement** (131 stations sur 9 949 au 08/11, aucun des codes 75056, 69123, 13055) ; l'agrégation au niveau commune passe par `COMPARENT` du COG. 0 code sans correspondance dans le COG.
- Filosofi : `MED_SL` (niveau de vie médian) est l'indicateur principal (disponible pour 88,6 % des communes, 99,8 % des stations), `PR_MD60` (taux de pauvreté) un complément (15,1 % des communes, 77,2 % des stations), à confirmer en J6 et J7.
- Pas de table `ref_cog_mouvement` pour l'instant (décision du 08/11).
- Flux annuel : candidat de backfill (décision en J4 ou J5). Fuseau de `maj`, licence et complétude non vérifiés. Le XML de 359 Mo se parse en flux, jamais en mémoire dans le conteneur qui collecte.
- SIRENE : retiré du périmètre le 16/11.
- Mention de licence à reprendre dans le README : « Source : Insee, Code officiel géographique 2026 » et « Source : Insee, Filosofi 2023 » (conditions de la Licence Ouverte 2.0). Le README n'existe pas encore.
- Contours des communes reportés en J11 si Qlik en a besoin (spec J6).
- J6 (17/11) : rattachement des stations par `com_arm_code` du quotidien, rechargé à la main (`make charger-station-commune` puis `make charger-referentiels-bronze`) ; code COM ou ARM d'abord, parent d'un COMD ou d'un COMA ensuite (un code peut être COM et COMD) ; couverture en error sous 99,27 % (mesure 99,777 %, dénominateur : stations de `stg_carburants__station_etats`) ; freshness du rattachement sur `fichier_date`.
- Référentiels au bronze : remplacement complet par table (`CREATE OR REPLACE TABLE ... AS SELECT * FROM read_files`), un Parquet par chargement dans `referentiels/` du Volume ; `_rescued_data` gardée et testée en error.
- dbt : staging en vue privée (`carburant_silver`), intermediate en vue `protected` (`carburant_silver`), marts en table `protected` (`carburant_gold`), contracts et `public` en J8. `charge_ts` = heure du bronze dans tous les modèles. Millésimes par variables (`cog_millesime`, `filosofi_millesime`).
- dbt (16/11) : dbt Core 1.12.6 et dbt-databricks 1.13.0 sur l'hôte dans `.venv-dbt` (Python 3.12), séparé de `.venv` (3.14) et de l'image Airflow ; `dbt/profiles.yml` versionné en `env_var` seulement ; macro `generate_schema_name` (schéma personnalisé tel quel) ; staging en vue, privé, groupe `carburants`, dans `carburant_silver` ; tests rattachés à `carburant_silver` ; garde pytest des schémas sur le manifest (`dbt parse` sans connexion). Jamais de `dbt run` ni de `dbt build` sans `--select` ; `dbt ls` des schémas avant un premier run.
- **`dbt/logs/` et `dbt/target/` ne sont jamais affichés en brut ni collés** (`dbt.log` contient l'hôte et le `http_path`) : recherche ciblée, hôte, `http_path` et identifiant d'entrepôt masqués.
- Freshness des sources bronze sur `collecte_ts` (fraîcheur de la donnée, pas `_charge_ts`) : warn 28 h, error 52 h (calcul au bloc 0b).
- Staging des états : coordonnées = clés `latitude` et `longitude` divisées par 100 000 (égales à `geom`, écart 0) ; ruptures, horaires et services gardés bruts (J7) ; `geom`, `services`, `horaires_jour` et les 12 clés de rupture par carburant non repris.
- Databricks : catalogue `workspace`, Volume `carburant_bronze.depot`, schémas `carburant_*`. Les schémas `bronze`, `silver`, `gold`, `ci_silver`, `ci_gold` et `default` appartiennent à un AUTRE projet (EnergiScope) : jamais touchés (test sur les noms qualifiés).
- Bootstrap : trois contrôles distincts (variables nommées, authentification dans un processus enfant tué après 30 s, entrepôt prêt par `SELECT 1` scruté au plus 5 fois), au plus 5 interrogations par requête, arrêt à la première erreur, aucune valeur sensible affichée. Un entrepôt serverless arrêté démarre à la première requête.
- Les quatre variables `DATABRICKS_HOST`, `DATABRICKS_TOKEN`, `DATABRICKS_CATALOG`, `DATABRICKS_WAREHOUSE_ID` sont lues dans l'environnement du processus (noms dans `.env.example`, valeurs dans `.env`). Pour `make bootstrap`, `make bootstrap-controles`, `make charger-bronze` et `make lire-bronze` seulement, make inclut `.env`. Dans le conteneur airflow, elles viennent de `environment:` du compose (`NOM: ${NOM}`).
- `charger` ne supprime plus ses Parquet (`4fa7b1f`) : un retry rejoue sans effet ; fichier absent sans confirmation au journal de purge : `LotAbsentError` qui nomme le lot, échec sans retry. Raison : `passage_flux` n'est pas exhaustif, il ne sert pas à déduire « déjà chargé ».
- Purge séparée, non branchée : lot de plus de 48 h ET contenu retrouvé dans Postgres (prix par clé, états par station et hash), confirmation écrite avant suppression, un lot non confirmé n'est jamais supprimé. Branchement à décider.
- Aucun rechargement ni purge des 20 lots d'avant le 09/11 (décision de l'utilisateur).
- Mutations : dans une copie hors dépôt, dans un conteneur jetable (`docker compose run --rm --no-deps ... bash -c`, `sh` est refusé par l'entrypoint d'Airflow), jamais dans le dossier monté ; `docker compose cp` vers le conteneur de collecte refusé (mémoire). Restauration prouvée par hash, identifiants des conteneurs et schémas `test_%` contrôlés avant et après.
- Chargement vers Databricks : DAG `chargement_bronze` (16/11), curseur par table dans `export_curseur`, `COPY INTO` avec liste exacte de fichiers, contrôle par fenêtre et lecture globale (`lire_bronze`). Recréation d'`airflow` faite le 16/11 (métadonnées sur Postgres `airflow`). Backfill du flux annuel non fait.

### Règles non négociables

- **`make reprise` ne recrée jamais un conteneur** : `docker compose up -d --no-recreate`. **`make up` (`up -d --build`) recrée** : il sert au premier démarrage ou à appliquer un changement de configuration, après un `pg_dump` frais et en connaissant la perte des métadonnées d'Airflow ; jamais pour reprendre après un `pause`. Preuve par les identifiants de conteneur avant et après.
- **`make collecte-run` écrit dans les vraies tables** (`airflow dags test collecte_flux`) : ce n'est pas un essai à blanc.
- Ne jamais lire ni afficher `.env`. Ne jamais afficher `DATABRICKS_HOST`, `DATABRICKS_TOKEN` ni l'identifiant d'entrepôt.
- Réponses, docs et commits en français. Jamais de tirets cadratin. Aucun Co-Authored-By.
- Commits conventionnels, petits, un sujet par commit, `git add` nominatif, `git diff --cached --stat` avant. Vérifier `git config user.email` = `737222+sebagitt@users.noreply.github.com`.
- Aucun secret dans le repo ni en sortie.
- Jamais `docker compose down -v` ni `down`. Jamais `make pause` avant la fin de séance.
- Pas de `make test` aux minutes :00, :15, :30, :45 (runs de collecte).
- **Pas de push sans accord** ; coller `git show --stat --oneline -3` avant. **Aucun commit ni push de `docs/fiches-source` sans accord explicite.**
- Avant d'exécuter un message qui cite des étapes déjà faites, recouper avec `git log` et l'état réel.
- Après une correction comprise : entrée de trois lignes dans `docs/journal/learnings.md`, cause étiquetée établie, hypothèse ou inconnue.
- Un test vert ne prouve pas qu'il protège une règle : pour les règles critiques, mutation d'une ligne dans une copie hors dépôt, le test doit passer au rouge (voir `docs/journal/J4.md`).
- Fin de journée : tests verts (`make test-tout`), debrief écrit, HANDOFF à jour, commit poussé (après accord).

## 4. Schémas

Base `collecte` (Postgres) :

```text
prix_flux(station_id BIGINT, carburant TEXT, maj TIMESTAMPTZ, prix NUMERIC(6,3), collecte_ts TIMESTAMPTZ)
  PK (station_id, carburant, maj), index sur collecte_ts, ON CONFLICT DO NOTHING

station_etat_flux(station_id BIGINT, collecte_ts TIMESTAMPTZ, etat_hash TEXT, etat JSONB)
  PK (station_id, collecte_ts)

passage_flux(collecte_ts, run_planifie, lignes_lues, lignes_inserees, etats_changes)
  une ligne par passage journalisé (non exhaustif)

ref_cog_commune(millesime, typecom, com, ... COMPARENT ...)  PK (millesime, typecom, com)  -- TYPECOM : COM, COMA, COMD, ARM
ref_cog_departement  PK (millesime, dep)      ref_cog_region  PK (millesime, reg)
ref_filosofi_commune(millesime, geo_object, codgeo, med_sl, med_sl_statut, med_sl_conf, pr_md60, pr_md60_statut, pr_md60_conf, charge_ts)
  PK (millesime, geo_object, codgeo) ; geo_object COM ou ARM
```

Databricks (catalogue `workspace`) : `carburant_bronze` (avec le Volume `depot`), `carburant_silver`, `carburant_gold`.

```text
carburant_bronze.prix_flux(station_id BIGINT, carburant STRING, maj TIMESTAMP, prix DECIMAL(6,3), collecte_ts TIMESTAMP,
                           _fichier_source STRING, _charge_ts TIMESTAMP)
carburant_bronze.station_etat_flux(station_id BIGINT, collecte_ts TIMESTAMP, etat_hash STRING, etat STRING (JSON, 33 clés),
                                   _fichier_source STRING, _charge_ts TIMESTAMP)
carburant_silver.stg_carburants__prix (vue) : prix_id, station_id, carburant, prix_maj_ts, prix_eur_litre DECIMAL(6,3),
                                   collecte_ts, fichier_source, charge_ts
carburant_silver.stg_carburants__station_etats (vue) : station_etat_id, station_id, collecte_ts, etat_hash, adresse, code_postal,
                                   ville, code_departement, departement_nom, code_region, region_nom, latitude DOUBLE,
                                   longitude DOUBLE, type_route_code, automate_24_24 BOOLEAN, horaires_json, services_json,
                                   ruptures_json, carburants_disponibles_json, carburants_indisponibles_json,
                                   carburants_rupture_definitive_liste, carburants_rupture_temporaire_liste,
                                   <carburant>_rupture_debut_ts TIMESTAMP et <carburant>_rupture_type STRING pour e10, e85,
                                   gazole, gplc, sp95, sp98 (12 colonnes, J8 ; début en heure de Paris converti en UTC),
                                   fichier_source, charge_ts
carburant_bronze.ref_cog_commune(millesime INT, typecom, com, reg, dep, ctcd, arr, tncc, ncc, nccenr, libelle, can, comparent STRING,
                                 charge_ts TIMESTAMP, _rescued_data STRING, _fichier_source STRING, _charge_ts TIMESTAMP)
carburant_bronze.ref_cog_departement(millesime INT, dep, reg, cheflieu, tncc, ncc, nccenr, libelle STRING, charge_ts TIMESTAMP,
                                     _rescued_data STRING, _fichier_source STRING, _charge_ts TIMESTAMP)
carburant_bronze.ref_cog_region(millesime INT, reg, cheflieu, tncc, ncc, nccenr, libelle STRING, charge_ts TIMESTAMP,
                                _rescued_data STRING, _fichier_source STRING, _charge_ts TIMESTAMP)
carburant_bronze.ref_filosofi_commune(millesime INT, geo_object STRING, codgeo STRING, med_sl DOUBLE, med_sl_statut STRING,
                                      med_sl_conf STRING, pr_md60 DOUBLE, pr_md60_statut STRING, pr_md60_conf STRING,
                                      charge_ts TIMESTAMP, _rescued_data STRING, _fichier_source STRING, _charge_ts TIMESTAMP)
carburant_bronze.ref_station_commune(station_id BIGINT, com_arm_code STRING, fichier_date DATE, charge_ts TIMESTAMP,
                                     _rescued_data STRING, _fichier_source STRING, _charge_ts TIMESTAMP)
  (charge_ts : heure Postgres ; _charge_ts : heure du bronze ; _rescued_data ajoutée par read_files, vide)
carburant_silver.stg_insee__cog_communes (vue) : commune_cle, cog_millesime, commune_code, commune_type, commune_parent_code,
                                   commune_nom, departement_code, region_code, postgres_charge_ts, charge_ts
carburant_silver.stg_insee__cog_departements (vue) : departement_code, departement_nom, region_code, cheflieu_commune_code,
                                   cog_millesime, postgres_charge_ts, charge_ts
carburant_silver.stg_insee__cog_regions (vue) : region_code, region_nom, cheflieu_commune_code, cog_millesime, postgres_charge_ts, charge_ts
carburant_silver.stg_insee__filosofi_communes (vue) : filosofi_cle, filosofi_millesime, geo_object, commune_code, med_sl, med_sl_statut,
                                   med_sl_conf, pr_md60, pr_md60_statut, pr_md60_conf, postgres_charge_ts, charge_ts
carburant_silver.stg_carburants__station_communes (vue) : station_id, commune_arm_code, fichier_date, postgres_charge_ts, charge_ts
carburant_silver.int_stations__commune (vue) : station_id, commune_arm_code, commune_code, rattachement_source,
                                   departement_code_flux, departement_code_cog
carburant_gold.dim_commune (table) : commune_code, commune_type, commune_nom, commune_parent_code, departement_code,
                                   departement_nom, region_code, region_nom, niveau_vie_median_eur, niveau_vie_median_statut,
                                   taux_pauvrete_pct, taux_pauvrete_statut, filosofi_millesime, cog_millesime
                                   (public, contract appliqué ; not_null : commune_code)
carburant_gold.dim_station (table, SCD2 par îlots, public, contract) : station_version_id STRING, station_id BIGINT,
                                   version_num BIGINT, adresse, code_postal, ville, code_departement, departement_nom,
                                   code_region, region_nom STRING, latitude, longitude DOUBLE, type_route_code STRING,
                                   automate_24_24 BOOLEAN, services ARRAY<STRING> (trié), debut_validite_ts TIMESTAMP,
                                   fin_validite_ts TIMESTAMP (NULL pour la courante), est_courante BOOLEAN, charge_ts TIMESTAMP
                                   grain (station_id, debut_validite_ts) ; not_null : station_version_id, station_id, debut_validite_ts
carburant_silver.int_ruptures__observations (vue, protected) : station_id BIGINT, collecte_ts TIMESTAMP, carburant STRING,
                                   rupture_debut_ts TIMESTAMP, rupture_type STRING
                                   une ligne par (station_id, collecte_ts, carburant) avec rupture présente
carburant_gold.fct_ruptures (table, public, contract) : rupture_id STRING, station_id BIGINT, carburant STRING,
                                   rupture_debut_ts TIMESTAMP, rupture_type STRING, premiere_observation_ts TIMESTAMP,
                                   derniere_observation_ts TIMESTAMP, fin_observee_ts TIMESTAMP, est_en_cours BOOLEAN,
                                   commune_code STRING
                                   grain (station_id, carburant, rupture_debut_ts) ; not_null : rupture_id, station_id,
                                   carburant, rupture_debut_ts, premiere_observation_ts
```

Base `collecte`, ajout du 17/11 : `ref_station_commune(station_id BIGINT PRIMARY KEY, com_arm_code TEXT, fichier_date DATE NOT NULL, charge_ts TIMESTAMPTZ NOT NULL DEFAULT now())`.

Fichiers de lots (hors base), dans `LOTS_DIR` (`/opt/airflow/data/lots` dans le conteneur, `data/lots` sur l'hôte) :

```text
prix_<horodatage>.parquet   (station_id, carburant, maj, prix)        horodatage = AAAAMMJJTHHMMSSZ en UTC
etat_<horodatage>.parquet   (station_id, etat_hash, etat)
purge_journal.jsonl         une ligne JSON par lot confirmé : lot, confirme, confirme_le, age_heures,
                            prix_lignes, prix_retrouvees, etats_lignes, etats_retrouves
                            (n'existe pas encore : la purge n'est pas branchée)
```

## 5. Commandes qui marchent

```text
make up                                  docker compose up -d --build (RECRÉE : premier démarrage ou changement de configuration, après pg_dump)
make pause                               docker compose stop (jamais avant la fin de séance)
make reprise                             up -d --no-recreate, unpause si besoin, 5 runs, comptes, 5 passages, autres piles, mémoire et PIDS, verdict
make test                                tests du conteneur (458), base de métadonnées sqlite dédiée
make test-bootstrap                      tests du bootstrap Databricks sur l'hôte, dans .venv (73)
make test-tout                           conteneur, hôte et garde dbt (535 le 17/11) : critère vert
make test-dbt-garde                      garde des schémas dbt (4), .venv-dbt, dbt parse sans connexion ni .env
make dbt ARGS="debug"                    dbt dans .venv-dbt (inclut .env pour cette cible, DBT_PROFILES_DIR et DBT_PROJECT_DIR sur dbt/)
make dbt ARGS="deps"                     installe dbt_utils 1.4.1 dans dbt/dbt_packages
make dbt ARGS="source freshness"         freshness de prix_flux, station_etat_flux, ref_station_commune (lecture seule)
make dbt ARGS="build --select staging"   ÉCRIT les 7 vues de staging puis teste (87 : PASS 86, WARN 1, le 17/11)
make dbt ARGS="build --select +dim_commune +int_stations__commune"   ÉCRIT staging Insee, dim_commune, int_stations__commune (92 : PASS 91, WARN 1)
make charger-station-commune             télécharge le quotidien, GX, remplace ref_station_commune dans Postgres (9 952 le 17/11)
make charger-referentiels-bronze ARGS="--table toutes"   5 référentiels vers carburant_bronze (conteneur jetable), entre :02 et :08 ; --table T pour une seule
make dbt ARGS="ls --resource-type model --output json --output-keys unique_id schema access group"   schémas avant un run
make dbt ARGS="run-operation decrire_colonnes"   colonnes et types des tables du bronze (lecture seule, refuse hors carburant_*)
make dbt ARGS="show --output json --inline \"select ...\""   lecture ; $$ pour le $ des chemins JSON, pas de limit
make charger-cog                         télécharge, valide (GX), remplace le millésime 2026 : 37 496, 101 et 18 lignes
make charger-filosofi                    télécharge, valide (GX), remplace le millésime 2023 : 34 813 lignes
make mesure-referentiels                 Filosofi dans le COG par TYPECOM, valeurs vides, 7 communes annoncées
make mesure-carburants M=m1              m1 à m5, m8, m9, m10, avec m4b (rupture par type) et m4c
                                         m9 : codes commune du quotidien dans le COG ; m10 : stations avec MED_SL et PR_MD60 renseignés
make rehash-etats [ARGS=--confirmer]     simulation par défaut, écrit seulement avec --confirmer
make collecte-run                        airflow dags test collecte_flux (ÉCRIT dans les vraies tables)
make bootstrap-controles                 variables, authentification, entrepôt prêt (aucune écriture)
make bootstrap                           les trois contrôles, puis le SQL deux fois avec SHOW SCHEMAS / SHOW VOLUMES avant, après le run 1, après le run 2
docker compose exec -T airflow airflow dags list-runs collecte_flux     runs récents : aucun running avant d'écrire un fichier de test
git ls-remote origin refs/heads/main     tête réelle du dépôt distant (lecture seule), à recouper avec git status -sb
make lire-bronze ARGS="--table toutes"   lecture seule du bronze : total, min, max, doublons ; --debut ISO --fin ISO pour une plage [debut, fin[
make charger-bronze ARGS="--table T --max-fenetres 48 --max-appels N"   chargement manuel (conteneur jetable), lancer entre :02 et :08
docker compose exec -T airflow ls -l /opt/airflow/simple_auth_manager_passwords.json.generated   mot de passe de l'interface : lire le fichier soi-même, jamais dans un rapport
```

Venv de l'hôte : `python -m venv .venv` puis `.venv/Scripts/pip install -r requirements-databricks.txt` (Python 3.14.3, versions épinglées).
Venv dbt : `py -3.12 -m venv .venv-dbt` puis `.venv-dbt/Scripts/pip install -r requirements-dbt.txt` (Python 3.12.10, versions épinglées).
Mutation dbt hors dépôt : copier `dbt/` sans `target`, `logs`, `garde` ; modèle : `make dbt DBT_PROJECT_DIR=<copie>/dbt ARGS="test --select <unit test>"` ; garde : `GARDE_PROJET_DIR=<copie>/dbt` puis `.venv-dbt/Scripts/python -m pytest dbt/garde`.

Reprise après un arrêt : `make reprise` (fonctionne sous PowerShell et Git Bash). Attendus : conteneurs `Running` sans `Recreate`, `is_paused : False`, 5 derniers runs, comptes, 5 derniers passages, « aucune autre pile compose en cours », mémoire sous 6 Gio, verdict. EN ATTENTE (dernier passage de plus de 20 minutes) est normal juste après la reprise : attendre le prochain quart d'heure et relancer. Après un arrêt long, le premier run peut déclencher le garde-fou des états. `KO : base injoignable` signifie que le dernier passage n'a pas pu être lu.

Sur Git Bash, préfixer `MSYS_NO_PATHCONV=1` pour les chemins de conteneur (par exemple `docker compose exec -w /opt/airflow/project`). Postgres n'est pas exposé sur l'hôte : tout passe par un conteneur. Les accents s'affichent mal dans la console Windows : `PYTHONUTF8=1` est posé dans les recettes concernées.

## 6. Erreurs résolues

- **Recréation involontaire le 2026-11-06 12:20:55 UTC** : un `make reprise` (avec `up -d` simple) lancé depuis PowerShell a détruit puis recréé `postgres` et `airflow`. Données conservées (volume `pg_data`), métadonnées d'Airflow perdues, run de 12:15 rejoué (passage en double à 12:21:19, 0 ligne). Cause **inconnue**. Correctif de précaution : `--no-recreate`.
- Garde-fou 30 % le 05/11 : il a **fonctionné** ; la source avait réordonné `services`. Correction : hash canonique et recalcul des hash stockés (13 460 lignes).
- `make reprise` sous PowerShell : `bash` du PATH est le lanceur WSL ; le Makefile utilise le bash de Git for Windows déduit de `git --exec-path`.
- `KO authentification` en une seconde : `TypeError` de mon code (`WorkspaceClient` n'accepte pas `http_timeout_seconds`, attribut de `Config`). Corrigé en TDD, test de la signature du vrai SDK.
- Mémoire d'Airflow : le `LocalExecutor` crée un worker par tâche jusqu'à `parallelism` (32) et ne les arrête jamais. Plafonné à 2.
- Fiche carburants : « aucun champ de fin » incomplet (`@fin` existe) ; « zéro y serait anormal » remplacé par des mesures de `passage_flux` (`2fce755`) ; types de rupture du flux annuel qui ne totalisent pas les éléments (42 sans type).
- Un message ancien exécuté par erreur (`928407d`, fiche modifiée sans accord) : corrigé, règle de recoupement dans `CLAUDE.md`.
- Un test collecté par erreur par pytest (`tester_...`) : renommé `controler_...`.
- `grep -P` échoue avec la locale : vérifier les caractères en Python.
- `charger` : `FileNotFoundError` au retry après un chargement terminé. Mécanisme **établi** (test rouge, lecture à `pd.read_parquet` après un `unlink`) ; déclencheur réel **hypothèse**, jamais observé. Correction : lots conservés, logique dans `ingestion/lots.py`, commit `4fa7b1f`.
- Tests de la purge : 4 mutants survivants sur 7 (M2, M1b, M3b, M4b) et un test (ligne 154 de `test_bootstrap_databricks.py`) qui ne prouvait pas la transmission du jeton. Un test par survivant, rouge sous la mutation puis vert (`83b498a`, `05494b7`, `848b170`, `6f281bb`, `f35c605`).
- `docker compose run ... sh` refusé par l'entrypoint d'Airflow (`invalid choice: 'sh'`) : utiliser `bash -c`.
- Comparaison d'arbre faussée par les `__pycache__` écrits dans la copie de mutation : exclure `__pycache__` et poser `PYTHONDONTWRITEBYTECODE=1`.
- Deux scripts d'attente lancés en double (`pkill` absent de ce shell) : tués par `kill` avant la mutation, aucune mutation en double.
- Journaux annoncés qui n'existaient pas : seul le cycle de test M3b a un journal complet ; M4b, M2, M1b et le test du jeton n'ont que les sorties collées dans le chat.

- J6 (17/11) : mutant G2 survivant (test qui n'attendait qu'un `ValueError` quelconque, il exige maintenant le message) ; `_rescued_data` inattendue après `read_files` (gardée et testée) ; ébauche TDD refusée par dbt tant qu'elle ne cite pas tous les `ref` donnés aux unit tests ; heures d'un rapport en heure locale (`date` sans `-u`), puis heures du matin converties à tort (voir `learnings.md`).

## 7. Points ouverts

- J8 : format de `debut` : si la source corrige son étiquette `+00:00`, la conversion depuis l'heure de Paris décalera tout d'une ou deux heures en silence ; le test `rupture_debut_ts <= premiere_observation_ts` ne voit que le cas inverse.
- J8 : `to_timestamp` en échec bruyant (mode ANSI du serverless) : une valeur de `debut` illisible fait échouer toute la vue du staging (voulu, documenté).
- J8 : unit test du fuseau sur le seul gazole ; les 5 autres carburants ne sont couverts que par le filet (une mutation vue, sur le gazole).
- J8 : station 45300010, département 45 dans le flux, commune 79202 (79) dans le quotidien : warn `int_stations_ecarts_departement` (0 au 17/11) ; erreur probable du quotidien, hypothèse.
- J8 : CTE d'import `select *` sur le staging dans `int_stations__commune` : sans effet sur ses colonnes (prouvé le 23/11), à revoir si ce modèle passe sous contract.
- J8 : `make charger-bronze` n'affiche que `ERREUR ErreurBootstrap` : le diagnostic `(<classe>, <error_code>)` n'est visible que dans le log d'Airflow.
- J8 : arrêt automatique de l'entrepôt non vérifié (sollicité jusqu'à 15:58 le 23/11).
- J7 : refus de `warehouses.start` du 19/11 (08:19, 08:24, 12:25 UTC) : cause toujours inconnue ; le prochain refus donnera la classe et l'`error_code` (`29adcfd`). Aucun refus depuis.
- J7 : seuils D3b à surveiller (faux positif si le taux d'une colonne volatile chute de moitié, aucun observé).

- J6 : Parquet orphelins dans `referentiels/` du Volume (chaque chargement en ajoute un, l'ancien reste ; `ref_cog_region` en a deux) : purge à décider.
- J6 : baisse lente de la couverture attendue (une station sortie du quotidien perd son rattachement au rechargement suivant) ; à suivre au prochain rechargement, seuil 99,27 % en error.
- J6 : Île-de-Sein et Île-Molène (Finistère) sans ligne Filosofi, absence non expliquée (fiche filosofi, piège 4) ; 105 communes d'outre-mer hors champ.
- J6 : rafraîchissement manuel des référentiels, dans l'ordre : `make charger-station-commune`, puis `make charger-referentiels-bronze ARGS="--table toutes"` (ou `--table ref_station_commune`), puis `make dbt ARGS="build --select +dim_commune +int_stations__commune"`.
- J6 : le connecteur SQL de dbt démarre l'entrepôt (une observation, 08:06:57 à 08:07:45 UTC le 17/11) : à confirmer.
- J6 : mutant MI de `int_stations__commune` équivalent ; à revoir si un code porte un jour plusieurs lignes COMD ou COMA (mesuré à 0).
- J6 : warn `not_null_stg_carburants__station_etats_code_departement` : 31 lignes pour 4 stations (cause probable : versions d'état accumulées, hypothèse).

- Pente de mémoire d'Airflow : 0,103 Gio par heure aux deux mesures (06/11, 08/11) avec `parallelism=2` ; cause résiduelle non cherchée.
- Cause de la recréation de 12:20 le 06/11 : inconnue.
- Cause du SIGKILL du 30/10 14:16:28 (run de 14:15) : hypothèse (mémoire de la machine virtuelle partagée avec l'autre pile).
- Rattrapage : 274 à 1 348 versions d'état par jour d'arrêt contre environ 3 700 en continu. Interprétation (changements transitoires invisibles après une coupure) non vérifiée.
- Filosofi : 7 communes en statut `O` hors liste de la page (six de la Meuse, Leménil-Mitry), cause non vérifiée (hypothèse : sans habitants) ; Île-de-Sein et Île-Molène sans ligne, absence non expliquée ; lien entre le vide de `PR_MD60` et la correction du 06/08/2026 non établi.
- Passages à zéro insertion : rejeux de créneau lus quelques secondes après un passage ; un cas isolé le 08/11 12:45:04 sans cause vérifiée.
- Purge non branchée : `data/lots` grossit (extrapolation d'environ 550 Mo par jour, non mesurée sur 24 h) ; un lot dont l'état n'a jamais été stocké ou dont le hash suit l'ancienne règle ne sera jamais purgé (lecture du code).
- `airflow/tests` non exécuté par `make test` (non monté dans le conteneur, chemins relatifs à la racine) : option B proposée (déplacer ces tests dans `ingestion/tests`), non décidée.
- Déclencheur réel d'un retry après un chargement terminé : inconnu (hypothèse non vérifiée : arrêt de la tâche après l'ancien `unlink`). Rejeu tardif d'un vieux lot : version d'état hors ordre possible (lecture du code, non exécuté).
- 8 lots du 23/10 (08:09 à 08:45) : `maj` décalée de 2 h ; cause probable fuseau (heure de Paris lue comme UTC), hypothèse, commit du correctif non cherché.
- Traces des cycles de test de J4 : scripts et journaux dans le scratchpad (hors dépôt, non versionnés) ; seul le cycle M3b a un journal complet.
- `requirements-dev.txt` non épinglé ; `pandas==2.1.4` et `great-expectations==1.24.1` indisponibles sous Python 3.14 (d'où `requirements-databricks.txt` séparé).
- Flaky `make test` : erreurs DNS occasionnelles.
- Certification Databricks à choisir (Data Analyst ou Data Engineer Associate) avant J9.
- À reporter dans le Guide : `services` reste dans le modèle, pas d'unicité (station, hash), couverture sur l'union des `collecte_ts`, `com_arm_code` par arrondissement pour Paris, Lyon et Marseille (option D retenue en J6, voir `docs/journal/J6.md`, section « À reporter dans le Guide »), `MED_SL` principal, flux annuel en backfill (J4 ou J5), SIRENE en documentation seule (Parquet : établissements et historique), schémas d'EnergiScope intouchables, règle `--no-recreate` et son exception, `make up` recrée, `make collecte-run` écrit dans les vraies tables, `parallelism=2`, `make test-tout`, `learnings.md`, `charger` sans `unlink` et purge confirmée par le contenu, mutations d'une ligne dans une copie hors dépôt (conteneur jetable), test de transmission du jeton, règle de fenêtre (`ingestion/` hors `tests/` et `airflow/dags/` en :01 à :14 puis hors run en cours ; `ingestion/tests/` hors :00, :15, :30, :45 et hors run en cours).
