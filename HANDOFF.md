# HANDOFF : fin de séance du 2026-11-09, dernière mesure à 16:31 (J4 clos sur le correctif de charger, la purge, les mutations et les tests ; chargement vers Databricks reporté en J5)

## 1. Tâche suivante : prochaine séance, dans cet ordre

1. `make reprise`. Premier verdict EN ATTENTE possible (normal), relancer au quart d'heure suivant. Comparer les identifiants de conteneur (section 2) avant et après : ils doivent être identiques, aucun `Recreate` dans la sortie. Après un arrêt, attendre un rattrapage (mesures : après environ 46 h d'arrêt, 10 737 prix et 1 266 états changés, soit 12,9 % des environ 9 800 stations ; après 22 h, 14 719 prix et 1 222 états ; après environ 18 h, au 08/11 à 15:00, 7 370 prix et 803 états, information de l'utilisateur).
2. Fin de séance : push après accord de l'utilisateur ; au début de la séance suivante, vérifier `git status -sb` (attendu : aucun ahead) et `git ls-remote origin refs/heads/main` (attendu : `f35c605` ou un commit plus récent). Avant tout push : `git status -sb`, `git log origin/main..HEAD --oneline`, `git diff origin/main..HEAD --stat`, noms de fichiers sans `.env`, `data/`, `.venv`, `docs/journal`, `docs/guide`, `.dump`, `.zip`, aucun Co-Authored-By, chemin local, secret ni tiret cadratin, et `git show --stat --oneline -3` collé.
3. J5 : **la spécification n'est pas accessible à Claude Code** ; spec à réécrire (non faite), avec le chargement vers Databricks prévu en premier bloc (information de l'utilisateur). Le chat donne le contenu (ne pas deviner). Avant tout code : la note de conception de l'étape 2, reportée de J4, sans code ni commande qui modifie un état :
   - (a) recréation d'`airflow` : accord conditionnel, une seule recréation, planifiée, après code et tests prêts, `pg_dump` frais, identifiants comparés avant et après, métadonnées d'Airflow déplacées vers la base `airflow` dans la même opération, accord à redemander au moment venu ;
   - (b) tableau de trois options : A reconstruire l'image (avec les métadonnées), B ajouter un service au compose sans toucher aux conteneurs existants, C script sur l'hôte ; pour chacune : ce qui est recréé, ce qui est perdu, conformité avec la spec (DAG de chargement dans Airflow), effort ; pour B, vérifier dans la documentation que `up -d --no-recreate` ajoute un service sans toucher aux autres, sans l'essayer sur la vraie pile ;
   - (c) jeton Databricks : options et risques, sans produire de `docker inspect` ni de `docker compose config` bruts ;
   - (d) export incrémental : source de vérité du curseur, borne semi-ouverte, marge avant le dernier passage terminé, noms de fichiers déterministes, reprise après échec à mi-chemin ;
   - (e) méthode de chargement : comportement face à un fichier ré-exporté sous le même nom avec un contenu différent ; quotas avec l'adresse de la page lue et la date de lecture ;
   - (f) concurrence (`parallelism=2`, le chargement ne doit pas retarder le run de 15 minutes) et estimation mémoire de l'export Parquet ;
   - (g) ordre de livraison : import du SDK dans les fonctions, DAG ajouté en dernier ; fixer `catchup` explicitement dans le DAG de chargement (`collecte_flux` a `catchup=False`, ce qui ne dit rien du DAG de chargement).
4. Backfill du flux annuel en J5, si le nommage et le curseur permettent une seconde source : voir la fiche carburants.
5. À décider : brancher la purge (`purger_lots`, non branchée, `data/lots` grossit) ; cible qui exécute `airflow/tests` (option B proposée : les déplacer dans `ingestion/tests`, non décidée).
6. Mémoire d'Airflow : remesurer la pente sur plusieurs heures (0,103 Gio par heure aux deux mesures, voir le tableau).
7. Clôture de la séance : debrief-jour, `make test-tout`, HANDOFF, push (après accord), `make pause` à un minute hors :00, :15, :30, :45.

J3 est clos : `make bootstrap` rejouable (deux passages, run 2 identique au run 1), COG et Filosofi chargés dans `collecte` avec GX, SIRENE en documentation seule (données en J8 si B-lite).

J4 est clos sur le correctif de `charger` (`4fa7b1f`), la purge confirmée par le contenu (écrite et testée, non branchée), les mutations de la purge et quatre tests ajoutés (`848b170`, `6f281bb`, `83b498a`, `05494b7`), et le test de transmission du jeton (`f35c605`). Le DAG de chargement vers Databricks n'est pas écrit : le critère initial de J4 n'est pas atteint, il est reporté en J5 par décision de l'utilisateur.

## 2. État (2026-11-09, dernière mesure : `make test-tout` passé à 16:31, heure de Paris)

- Collecte en marche (DAG `collecte_flux`, toutes les 15 minutes, `catchup=False`), `is_paused` False. Arrêt du 06/11 14:30:07 au 08/11 12:29:26 (environ 46 h), repris sans recréation. Reprise du 09/11 sans recréation (information de l'utilisateur) ; identifiants des conteneurs identiques jusqu'à 16:13:45 (mesuré), `StartedAt` `2026-11-09T08:50:45Z`, `RestartCount` 0 (lu à 12:42). Les 10 runs les plus récents lus à 15:22 et les 5 lus à 16:12 étaient `success`.
- Tests : `make test` (conteneur) 133 passés, `make test-bootstrap` (hôte) 73 passés, soit 206 pour `make test-tout` (mesuré à 16:31, code retour 0).
- Dépôt : `origin/main` à `f35c605` (`git ls-remote` à 16:30:36), aucun ahead. Commits de J4 : `4fa7b1f` (correctif de `charger`), `848b170`, `6f281bb`, `83b498a`, `05494b7` (tests de la purge), `f35c605` (test de transmission du jeton). Les cinq premiers ont été poussés à 15:46:08 ; `f35c605` a été poussé par l'utilisateur depuis VS Code, après contrôle du diff (heure non relevée).
- Lots Parquet : `charger` ne supprime plus ses fichiers (logique dans `ingestion/lots.py`). `purger_lots` (âge supérieur à 48 h ET prix par clé et états par station et hash retrouvés dans Postgres, confirmation écrite dans `purge_journal.jsonl`) **n'est branchée nulle part**. `data/lots` grossit (16:31 : 78 fichiers, 193 168 Kio ; les 48 fichiers du 09/11 pèsent 137 590 639 octets ; extrapolation, non mesurée sur 24 h : environ 550 Mo par jour). Les 20 lots d'avant le 09/11 (12 retrouvés par contenu dont 7 lots du 05/11 dont les états avaient été bloqués par le garde-fou, 8 du 23/10 avec une `maj` décalée de 2 h) ne sont ni rechargés ni purgés (décision de l'utilisateur).
- Tables de référence dans la base `collecte` : `ref_cog_commune` 37 496 lignes (millésime 2026), `ref_cog_departement` 101, `ref_cog_region` 18, `ref_filosofi_commune` 34 813 lignes (millésime 2023, 34 768 communes plus 45 arrondissements). Clé de `ref_filosofi_commune` : (`millesime`, `geo_object`, `codgeo`).
- Databricks (catalogue `workspace`) : schémas `carburant_bronze`, `carburant_silver`, `carburant_gold` et Volume `carburant_bronze.depot` créés le 06/11. Les 7 schémas existants (`bronze`, `silver`, `gold`, `ci_silver`, `ci_gold`, `default`, `information_schema`) sont inchangés. Token valide jusqu'au 2027-06-17 (information de l'utilisateur).
- Airflow : `AIRFLOW__CORE__PARALLELISM=2` (commit `0695b7c`). Métadonnées d'Airflow recréées le 06/11 à 13:00 (historique des runs avant cette heure perdu).
- Pile EnergiScope : projet terminé et publié, ses 7 conteneurs sont à `restart=no` et arrêtés. `make reprise` avertit si une autre pile compose tourne.
- Sauvegardes (`data/backups`, ignoré par git) : `collecte_20261108T145530Z.dump` (6 974 655 octets, UTC), pris à l'arrêt propre du 08/11 ; `collecte_20261106T130019Z.dump` (avant la recréation d'airflow) ; `collecte_20261105T143645Z.dump`. Faire un `pg_dump` frais avant toute recréation.
- Pas d'hôte 24/7 : la collecte s'arrête avec le projet (jusqu'à J15 plus 7 jours), à écrire dans le README.
- Fiches : `docs/fiches-source/carburants.md` (mise à jour du 08/11), `cog.md`, `filosofi.md`, `sirene.md`.
- Journal de leçons : `docs/journal/learnings.md` (ignoré par git, 16 entrées, cause étiquetée établie, hypothèse ou inconnue). Détail des jours : `docs/journal/J3.md`, `docs/journal/J4.md` et `docs/journal/J4_mutations_purge.md` (ignorés).
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

- Logique dans `ingestion/` et `quality/gx/` (pytest), DAG mince dans `airflow/dags/`. `ingestion/` et `airflow/dags/` sont montés dans le conteneur : une édition cassée arrête la collecte. `ingestion/` hors `tests/` (dont `lots.py`) et `airflow/dags/` : écriture en fenêtre :01 à :14 de chaque quart d'heure, puis hors run en cours (`airflow dags list-runs collecte_flux`, aucun `running`) ; contrôler l'import, tester. `ingestion/tests/` : écriture hors :00, :15, :30, :45 et hors run en cours.
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
- `charger` ne supprime plus ses Parquet (`4fa7b1f`) : un retry rejoue sans effet ; fichier absent sans confirmation au journal de purge : `LotAbsentError` qui nomme le lot, échec sans retry. Raison : `passage_flux` n'est pas exhaustif, il ne sert pas à déduire « déjà chargé ».
- Purge séparée, non branchée : lot de plus de 48 h ET contenu retrouvé dans Postgres (prix par clé, états par station et hash), confirmation écrite avant suppression, un lot non confirmé n'est jamais supprimé. Branchement à décider.
- Aucun rechargement ni purge des 20 lots d'avant le 09/11 (décision de l'utilisateur).
- Mutations : dans une copie hors dépôt, dans un conteneur jetable (`docker compose run --rm --no-deps ... bash -c`, `sh` est refusé par l'entrypoint d'Airflow), jamais dans le dossier monté ; `docker compose cp` vers le conteneur de collecte refusé (mémoire). Restauration prouvée par hash, identifiants des conteneurs et schémas `test_%` contrôlés avant et après.
- Chargement vers Databricks, note de conception et backfill du flux annuel : J5 (backfill si le nommage et le curseur permettent une seconde source). `catchup` du DAG de chargement à fixer explicitement. Recréation d'`airflow` : accord conditionnel, une seule, planifiée, avec `pg_dump` frais et métadonnées déplacées vers la base `airflow`, accord à redemander.

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

Databricks (catalogue `workspace`) : `carburant_bronze` (avec le Volume `depot`), `carburant_silver`, `carburant_gold`. Les tables de référence sont dans Postgres `collecte` pour l'instant ; leur chargement vers Databricks est à faire (DAG de chargement, J5).

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
make test                                tests du conteneur (133)
make test-bootstrap                      tests du bootstrap Databricks sur l'hôte, dans .venv (73)
make test-tout                           conteneur et hôte (206) : critère vert
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
- `charger` : `FileNotFoundError` au retry après un chargement terminé. Mécanisme **établi** (test rouge, lecture à `pd.read_parquet` après un `unlink`) ; déclencheur réel **hypothèse**, jamais observé. Correction : lots conservés, logique dans `ingestion/lots.py`, commit `4fa7b1f`.
- Tests de la purge : 4 mutants survivants sur 7 (M2, M1b, M3b, M4b) et un test (ligne 154 de `test_bootstrap_databricks.py`) qui ne prouvait pas la transmission du jeton. Un test par survivant, rouge sous la mutation puis vert (`83b498a`, `05494b7`, `848b170`, `6f281bb`, `f35c605`).
- `docker compose run ... sh` refusé par l'entrypoint d'Airflow (`invalid choice: 'sh'`) : utiliser `bash -c`.
- Comparaison d'arbre faussée par les `__pycache__` écrits dans la copie de mutation : exclure `__pycache__` et poser `PYTHONDONTWRITEBYTECODE=1`.
- Deux scripts d'attente lancés en double (`pkill` absent de ce shell) : tués par `kill` avant la mutation, aucune mutation en double.
- Journaux annoncés qui n'existaient pas : seul le cycle de test M3b a un journal complet ; M4b, M2, M1b et le test du jeton n'ont que les sorties collées dans le chat.

## 7. Points ouverts

- Pente de mémoire d'Airflow : 0,103 Gio par heure aux deux mesures (06/11, 08/11) avec `parallelism=2` ; cause résiduelle non cherchée.
- Cause de la recréation de 12:20 le 06/11 : inconnue.
- Cause du SIGKILL du 30/10 14:16:28 (run de 14:15) : hypothèse (mémoire de la machine virtuelle partagée avec l'autre pile).
- Rattrapage : 274 à 1 348 versions d'état par jour d'arrêt contre environ 3 700 en continu. Interprétation (changements transitoires invisibles après une coupure) non vérifiée.
- Filosofi : 7 communes en statut `O` hors liste de la page (six de la Meuse, Leménil-Mitry), cause non vérifiée (hypothèse : sans habitants) ; Île-de-Sein et Île-Molène sans ligne, absence non expliquée ; lien entre le vide de `PR_MD60` et la correction du 06/08/2026 non établi.
- NAF 2025 applicable en janvier 2027 : documenté, non revérifié dans les fichiers. Documentation de l'API Sirene non lue (valeurs de `etatAdministratifEtablissement`, `statutDiffusionEtablissement`, format du code d'activité).
- Passages à zéro insertion : rejeux de créneau lus quelques secondes après un passage ; un cas isolé le 08/11 12:45:04 sans cause vérifiée.
- Purge non branchée : `data/lots` grossit (extrapolation d'environ 550 Mo par jour, non mesurée sur 24 h) ; un lot dont l'état n'a jamais été stocké ou dont le hash suit l'ancienne règle ne sera jamais purgé (lecture du code).
- `airflow/tests` non exécuté par `make test` (non monté dans le conteneur, chemins relatifs à la racine) : option B proposée (déplacer ces tests dans `ingestion/tests`), non décidée.
- Déclencheur réel d'un retry après un chargement terminé : inconnu (hypothèse non vérifiée : arrêt de la tâche après l'ancien `unlink`). Rejeu tardif d'un vieux lot : version d'état hors ordre possible (lecture du code, non exécuté).
- 8 lots du 23/10 (08:09 à 08:45) : `maj` décalée de 2 h ; cause probable fuseau (heure de Paris lue comme UTC), hypothèse, commit du correctif non cherché.
- Traces des cycles de test de J4 : scripts et journaux dans le scratchpad (hors dépôt, non versionnés) ; seul le cycle M3b a un journal complet.
- Spécification de J5 : à réécrire (non faite), le chargement vers Databricks est prévu en premier bloc.
- Push de `f35c605` : fait par l'utilisateur depuis VS Code, après contrôle du diff, heure non relevée.
- Métadonnées d'Airflow encore dans le conteneur : à passer sur la base Postgres `airflow`, dans l'unique recréation planifiée d'`airflow` (accord conditionnel, à redemander).
- `requirements-dev.txt` non épinglé ; `pandas==2.1.4` et `great-expectations==1.24.1` indisponibles sous Python 3.14 (d'où `requirements-databricks.txt` séparé).
- Durée d'un démarrage à froid de l'entrepôt serverless : non mesurée (environ une minute au premier contrôle).
- Flaky `make test` : erreurs DNS occasionnelles.
- Certification Databricks à choisir (Data Analyst ou Data Engineer Associate) avant J9.
- À reporter dans le Guide : `services` reste dans le modèle, pas d'unicité (station, hash), couverture sur l'union des `collecte_ts`, `com_arm_code` par arrondissement pour Paris, Lyon et Marseille (jointure spatiale en J6), `MED_SL` principal, flux annuel en backfill (J4 ou J5), SIRENE en documentation seule (Parquet : établissements et historique), schémas d'EnergiScope intouchables, règle `--no-recreate` et son exception, `make up` recrée, `make collecte-run` écrit dans les vraies tables, `parallelism=2`, `make test-tout`, `learnings.md`, `charger` sans `unlink` et purge confirmée par le contenu, mutations d'une ligne dans une copie hors dépôt (conteneur jetable), test de transmission du jeton, règle de fenêtre (`ingestion/` hors `tests/` et `airflow/dags/` en :01 à :14 puis hors run en cours ; `ingestion/tests/` hors :00, :15, :30, :45 et hors run en cours).
