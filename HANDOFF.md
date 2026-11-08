# HANDOFF : fin de séance du 2026-11-08, dernière mesure à 15:25 (J3 clos : bootstrap, COG, Filosofi, SIRENE en documentation)

## 1. Tâche suivante : prochaine séance, dans cet ordre

1. `make reprise`. Premier verdict EN ATTENTE possible (normal), relancer au quart d'heure suivant. Comparer les identifiants de conteneur (section 2) avant et après : ils doivent être identiques, aucun `Recreate` dans la sortie. Après un arrêt, attendre un rattrapage (mesures : après environ 46 h d'arrêt, 10 737 prix et 1 266 états changés, soit 12,9 % des environ 9 800 stations ; après 22 h, 14 719 prix et 1 222 états).
2. Fin de séance : push après accord de l'utilisateur ; au début de la séance suivante, vérifier `git status -sb` (attendu : aucun ahead). Avant tout push : `git status -sb`, `git log origin/main..HEAD --oneline`, `git diff origin/main..HEAD --stat`, noms de fichiers sans `.env`, `data/`, `.venv`, `docs/journal`, `docs/guide`, `.dump`, `.zip`, aucun Co-Authored-By, chemin local, secret ni tiret cadratin, et `git show --stat --oneline -3` collé.
3. Début de J4 : correctif de `charger` (FileNotFoundError au retry après un chargement terminé), en TDD, commit séparé, juste après la fin d'un run ; puis DAG de chargement vers Databricks. **La spécification n'est pas accessible à Claude Code** : le chat donne le contenu de J4 (ne pas deviner).
4. Décision du backfill du flux annuel (J4 ou J5) : voir la fiche carburants.
5. Mémoire d'Airflow : remesurer la pente sur plusieurs heures (0,103 Gio par heure aux deux mesures, voir le tableau).
6. Clôture de la séance : debrief-jour, `make test-tout`, HANDOFF, push (après accord), `make pause` à un minute hors :00, :15, :30, :45.

J3 est clos : `make bootstrap` rejouable (deux passages, run 2 identique au run 1), COG et Filosofi chargés dans `collecte` avec GX, SIRENE en documentation seule (données en J8 si B-lite).

## 2. État (2026-11-08, dernière mesure : tests passés à 15:25, heure de Paris)

- Collecte en marche (DAG `collecte_flux`, toutes les 15 minutes), `is_paused` False. Arrêt du 06/11 14:30:07 au 08/11 12:29:26 (environ 46 h), repris sans recréation.
- Tests : `make test` (conteneur) 117 passés, `make test-bootstrap` (hôte) 72 passés, soit 189 pour `make test-tout`.
- Tables de référence dans la base `collecte` : `ref_cog_commune` 37 496 lignes (millésime 2026), `ref_cog_departement` 101, `ref_cog_region` 18, `ref_filosofi_commune` 34 813 lignes (millésime 2023, 34 768 communes plus 45 arrondissements). Clé de `ref_filosofi_commune` : (`millesime`, `geo_object`, `codgeo`).
- Databricks (catalogue `workspace`) : schémas `carburant_bronze`, `carburant_silver`, `carburant_gold` et Volume `carburant_bronze.depot` créés le 06/11. Les 7 schémas existants (`bronze`, `silver`, `gold`, `ci_silver`, `ci_gold`, `default`, `information_schema`) sont inchangés. Token valide jusqu'au 2027-06-17 (information de l'utilisateur).
- Airflow : `AIRFLOW__CORE__PARALLELISM=2` (commit `0695b7c`). Métadonnées d'Airflow recréées le 06/11 à 13:00 (historique des runs avant cette heure perdu).
- Pile EnergiScope : projet terminé et publié, ses 7 conteneurs sont à `restart=no` et arrêtés. `make reprise` avertit si une autre pile compose tourne.
- Sauvegardes (`data/backups`, ignoré par git) : `collecte_20261106T130019Z.dump` (avant la recréation d'airflow), `collecte_20261105T143645Z.dump`. Aucun `pg_dump` plus récent : en faire un avant toute recréation.
- Pas d'hôte 24/7 : la collecte s'arrête avec le projet (jusqu'à J15 plus 7 jours), à écrire dans le README.
- Fiches : `docs/fiches-source/carburants.md` (mise à jour du 08/11), `cog.md`, `filosofi.md`, `sirene.md`.
- Journal de leçons : `docs/journal/learnings.md` (ignoré par git, 14 entrées amorcées à la clôture de J3, cause étiquetée établie, hypothèse ou inconnue). Détail du jour : `docs/journal/J3.md` (ignoré).
- Règles de pédagogie : section « Pédagogie et leçons » de `CLAUDE.md` (expliquer en une ou deux lignes les commandes qui modifient un état, entrée de trois lignes dans `learnings.md` après chaque correction comprise, recouper les messages qui citent des étapes déjà faites, aucun commit de `docs/fiches-source` sans accord).

### Identifiants des conteneurs (à comparer après `make pause` puis `make reprise`)

```text
observatoire-carburant-airflow-1   6bc98c619e4fae2764e5f02e87f2f69b9acc05b92793f406a37e131ac7921c76   created 2026-11-06T13:00:31Z
observatoire-carburant-postgres-1  9b3b0a0f2f3f4fa44de11f957919be837b800d6054ded613a5631df8c1805c9d   created 2026-11-06T12:20:55Z
volume                             observatoire-carburant_pg_data
```
Inchangés après l'arrêt de 46 h (vérifié le 08/11). Commande : `docker inspect observatoire-carburant-airflow-1 observatoire-carburant-postgres-1 --format "{{.Name}} {{.Id}}"`. Si un identifiant change, un conteneur a été recréé : incident (métadonnées d'Airflow perdues, DAG en pause).

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

Critère provisoire : au moins 5 relevés sur 90 minutes ou plus, 2 workers au plus, pente sous 0,1 Gio par heure, alerte au-dessus de 3,5 Gio (jamais franchie, maximum 2,262 Gio). Pente (moindres carrés) : **0,103 Gio par heure le 06/11 et 0,103 le 08/11** (13:05 à 14:27, 4 relevés, 82 minutes ; 0,233 avec le relevé de 12:35 pris avant que les workers existent, point biaisé) : le seuil de 0,1 n'est pas tenu, de très peu. Un worker pèse environ 200 Mio de RSS. Cause de la pente résiduelle : hypothèse, non cherchée.

## 3. Décisions d'architecture

- Logique dans `ingestion/` et `quality/gx/` (pytest), DAG mince dans `airflow/dags/`. `ingestion/` et `airflow/dags/` sont montés dans le conteneur : une édition cassée arrête la collecte. Éditer juste après la fin d'un run, contrôler l'import, tester.
- Hash d'état canonique (listes triées, JSON dans les chaînes parsé). Garde-fou à 30 % de stations changées par passage : **il fonctionne** (le 05/11 il a bloqué un lot parce que la source avait réordonné `services`, l'empreinte l'ignore maintenant). `services` reste dans le modèle (SCD2 `dim_station` prévu par la spec).
- Pas d'unicité sur (station, hash) : une station peut revenir à un état déjà vu.
- `passage_flux` non exhaustif (écriture non bloquante). Couverture calculée sur l'union des `collecte_ts` de `prix_flux` et `passage_flux`.
- `com_arm_code` du quotidien : **Paris, Lyon et Marseille sont codées par arrondissement** (131 stations sur 9 949 au 08/11, aucun des codes 75056, 69123, 13055) ; l'agrégation au niveau commune passe par `COMPARENT` du COG. 0 code sans correspondance dans le COG. La décision de la jointure spatiale reste en J6.
- Filosofi : `MED_SL` (niveau de vie médian) est l'indicateur principal (disponible pour 88,6 % des communes, 99,8 % des stations), `PR_MD60` (taux de pauvreté) un complément (15,1 % des communes, 77,2 % des stations), à confirmer en J6 et J7.
- Pas de table `ref_cog_mouvement` pour l'instant (décision du 08/11).
- Flux annuel : candidat de backfill (décision en J4 ou J5). Fuseau de `maj`, licence et complétude non vérifiés. Le XML de 359 Mo se parse en flux, jamais en mémoire dans le conteneur qui collecte.
- SIRENE : documentation seule en J3 ; données en J8 si B-lite, en **Parquet**, deux fichiers : stock des établissements (2,2 Go) pour les créations et historique des établissements (882 Mo) pour les cessations. Pas de variable « date de cessation » : elle se déduit de l'historique.
- Mention de licence à reprendre dans le README : « Source : Insee, Code officiel géographique 2026 » et « Source : Insee, Filosofi 2023 » (conditions de la Licence Ouverte 2.0). Le README n'existe pas encore.
- Contours des communes reportés en J6.
- Databricks : catalogue `workspace`, Volume `carburant_bronze.depot`, schémas `carburant_*`. Les schémas `bronze`, `silver`, `gold`, `ci_silver`, `ci_gold` et `default` appartiennent à un AUTRE projet (EnergiScope) : jamais touchés (test sur les noms qualifiés).
- Bootstrap : trois contrôles distincts (variables nommées, authentification dans un processus enfant tué après 30 s, entrepôt prêt par `SELECT 1` scruté au plus 5 fois), au plus 5 interrogations par requête, arrêt à la première erreur, aucune valeur sensible affichée. Un entrepôt serverless arrêté démarre à la première requête.
- Les quatre variables `DATABRICKS_HOST`, `DATABRICKS_TOKEN`, `DATABRICKS_CATALOG`, `DATABRICKS_WAREHOUSE_ID` sont lues dans l'environnement du processus (noms dans `.env.example`, valeurs dans `.env`). Pour `make bootstrap` et `make bootstrap-controles` seulement, make inclut `.env`.

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

Databricks (catalogue `workspace`) : `carburant_bronze` (avec le Volume `depot`), `carburant_silver`, `carburant_gold`. Les tables de référence sont dans Postgres `collecte` pour l'instant ; leur chargement vers Databricks est à faire (DAG de chargement, J4).

## 5. Commandes qui marchent

```text
make up                                  docker compose up -d --build (RECRÉE : premier démarrage ou changement de configuration, après pg_dump)
make pause                               docker compose stop (jamais avant la fin de séance)
make reprise                             up -d --no-recreate, unpause si besoin, 5 runs, comptes, 5 passages, autres piles, mémoire et PIDS, verdict
make test                                tests du conteneur (117)
make test-bootstrap                      tests du bootstrap Databricks sur l'hôte, dans .venv (72)
make test-tout                           conteneur et hôte (189) : critère vert
make charger-cog                         télécharge, valide (GX), remplace le millésime 2026 : 37 496, 101 et 18 lignes
make charger-filosofi                    télécharge, valide (GX), remplace le millésime 2023 : 34 813 lignes
make mesure-referentiels                 Filosofi dans le COG par TYPECOM, valeurs vides, 7 communes annoncées
make mesure-carburants M=m1              m1 à m5, m8, m9, m10, avec m4b (rupture par type) et m4c
                                         m9 : codes commune du quotidien dans le COG ; m10 : stations avec MED_SL et PR_MD60 renseignés
make rehash-etats [ARGS=--confirmer]     simulation par défaut, écrit seulement avec --confirmer
make collecte-run                        airflow dags test collecte_flux (ÉCRIT dans les vraies tables)
make bootstrap-controles                 variables, authentification, entrepôt prêt (aucune écriture)
make bootstrap                           les trois contrôles, puis le SQL deux fois avec SHOW SCHEMAS / SHOW VOLUMES avant, après le run 1, après le run 2
```

Venv de l'hôte : `python -m venv .venv` puis `.venv/Scripts/pip install -r requirements-databricks.txt` (Python 3.14.3, versions épinglées).

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

## 7. Points ouverts

- Pente de mémoire d'Airflow : 0,103 Gio par heure aux deux mesures (06/11, 08/11) avec `parallelism=2` ; cause résiduelle non cherchée.
- Cause de la recréation de 12:20 le 06/11 : inconnue.
- Cause du SIGKILL du 30/10 14:16:28 (run de 14:15) : hypothèse (mémoire de la machine virtuelle partagée avec l'autre pile).
- Rattrapage : 274 à 1 348 versions d'état par jour d'arrêt contre environ 3 700 en continu. Interprétation (changements transitoires invisibles après une coupure) non vérifiée.
- Filosofi : 7 communes en statut `O` hors liste de la page (six de la Meuse, Leménil-Mitry), cause non vérifiée (hypothèse : sans habitants) ; Île-de-Sein et Île-Molène sans ligne, absence non expliquée ; lien entre le vide de `PR_MD60` et la correction du 06/08/2026 non établi.
- NAF 2025 applicable en janvier 2027 : documenté, non revérifié dans les fichiers. Documentation de l'API Sirene non lue (valeurs de `etatAdministratifEtablissement`, `statutDiffusionEtablissement`, format du code d'activité).
- Passages à zéro insertion : rejeux de créneau lus quelques secondes après un passage ; un cas isolé le 08/11 12:45:04 sans cause vérifiée.
- `charger` : FileNotFoundError au retry après un chargement terminé (début de J4).
- Métadonnées d'Airflow encore dans le conteneur : à passer sur la base Postgres `airflow`.
- `requirements-dev.txt` non épinglé ; `pandas==2.1.4` et `great-expectations==1.24.1` indisponibles sous Python 3.14 (d'où `requirements-databricks.txt` séparé).
- Durée d'un démarrage à froid de l'entrepôt serverless : non mesurée (environ une minute au premier contrôle).
- Flaky `make test` : erreurs DNS occasionnelles.
- Certification Databricks à choisir (Data Analyst ou Data Engineer Associate) avant J9.
- À reporter dans le Guide : `services` reste dans le modèle, pas d'unicité (station, hash), couverture sur l'union des `collecte_ts`, `com_arm_code` par arrondissement pour Paris, Lyon et Marseille (jointure spatiale en J6), `MED_SL` principal, flux annuel en backfill (J4 ou J5), SIRENE en documentation seule (Parquet : établissements et historique), schémas d'EnergiScope intouchables, règle `--no-recreate` et son exception, `make up` recrée, `make collecte-run` écrit dans les vraies tables, `parallelism=2`, `make test-tout`, `learnings.md`.
