# Fiche-source : prix des carburants (flux instantané v2 et flux quotidien)

Brouillon du 2026-11-05, mesures M1 à M5 et M8 faites, M6 et M7 non mesurées (hors périmètre allégé de J3). Chaque résultat porte une étiquette :
**mesuré** (commande de rejeu donnée), **documenté** (lu dans une documentation ou des métadonnées) ou **non vérifié**.
Rejeu de toutes les mesures, dans le conteneur : `make mesure-carburants M=m1` (m1 à m5, m8, avec m4b et m4c). Code : `ingestion/mesures_carburants.py`.

## 1. Identité

| | Flux instantané v2 | Flux quotidien |
|---|---|---|
| Jeu | `prix-des-carburants-en-france-flux-instantane-v2` | `prix-carburants-quotidien` |
| Éditeur | DGCCRF (documenté, métadonnées) | DGCCRF (documenté) |
| Portail | data.economie.gouv.fr, export JSON (`FLUX_URL` dans `ingestion/carburants.py`) | data.economie.gouv.fr, API Explore v2.1 |
| Autre diffusion | `donnees.roulez-eco.fr/opendata/instantane` (zip) | `donnees.roulez-eco.fr/opendata/jour` (zip), `annee` (zip annuel) |
| Contenu | une ligne par station, prix et attributs à plat | plusieurs lignes par station (7,6 en moyenne, **mesuré** : prix et ruptures sont éclatés), structure exacte **non vérifiée** |

Date de consultation : 2026-11-05.

## 2. Licence

- Instantané v2 : Licence Ouverte v2.0 (Etalab). **Documenté** (métadonnées du jeu).
- Quotidien : « Licence Ouverte (Etalab) », sans numéro de version dans les métadonnées. **Documenté**.
- Fichiers de `donnees.roulez-eco.fr` : conditions de réutilisation **non vérifiées**.

## 3. Fraîcheur

- Instantané v2 : champ `modified` du jeu à 10:36:43 UTC le 2026-11-05, lu à 10:40 UTC. **Mesuré**. Fréquence annoncée : toutes les 10 minutes, **documenté** lors de la reconnaissance du 23/10, non revérifié aujourd'hui (le champ `update_frequency` vaut `OTHER`). Le zip `instantané` de roulez-eco portait la date 10:40:14 GMT. **Mesuré** (HEAD).
- Quotidien : `modified` à 07:06:07 UTC le 2026-11-05, dernier `prix_maj` à 2026-11-04 23:59 UTC. **Mesuré** (M4).
- Latence de publication par ligne (`collecte_ts` moins `maj`) : médiane 30 minutes, p95 39, minimum 15,2, hors rattrapage (**mesuré**, M5, section 10). Notre collecte interroge toutes les 15 minutes, ce qui ajoute jusqu'à 15 minutes à cette latence.

## 4. Schéma

Mesuré par `make mesure-carburants M=m1` sur 9 826 stations le 2026-11-05 à 10:39 UTC. « Vide » veut dire absent, null, chaîne vide, liste vide ou objet vide. Types lus dans l'export JSON (pas ceux annoncés par le portail).

| Champ(s) | Type JSON | Vide | Remarque |
|---|---|---|---|
| `id` | entier | 0,0 % | clé de la station |
| `latitude`, `longitude` | chaîne | 0,0 % | ni nombres ni coordonnées de `geom` |
| `geom` | objet | 0,0 % | `{lon, lat}` |
| `adresse`, `cp`, `ville`, `pop` | chaîne | 0,0 % | `cp` code postal, pas de code commune |
| `departement`, `code_departement`, `region`, `code_region` | chaîne | 0,0 % | pas de code commune INSEE |
| `gazole_prix` / `gazole_maj` | nombre / chaîne | 7,5 % | `maj` : heure de Paris étiquetée UTC (voir pièges) |
| `e10_prix` / `e10_maj` | nombre / chaîne | 28,3 % | |
| `sp98_prix` / `sp98_maj` | nombre / chaîne | 30,3 % | |
| `e85_prix` / `e85_maj` | nombre / chaîne | 61,8 % | |
| `sp95_prix` / `sp95_maj` | nombre / chaîne | 72,4 % | |
| `gplc_prix` / `gplc_maj` | nombre / chaîne | 84,8 % | |
| `prix` | chaîne contenant du JSON | 4,8 % | doublon des champs `*_prix` |
| `rupture` | chaîne contenant du JSON | 7,4 % | liste d'entrées `@nom`, `@id`, `@debut`, `@fin`, `@type` |
| `<carburant>_rupture_debut`, `<carburant>_rupture_type` | chaîne | de 26,8 % (gplc) à 93,8 % (gazole) | un couple par carburant |
| `carburants_rupture_temporaire`, `carburants_rupture_definitive` | chaîne | 73,7 % et 21,4 % | |
| `carburants_disponibles`, `carburants_indisponibles` | liste | 4,8 % et 6,0 % | |
| `services`, `services_service` | chaîne contenant du JSON, liste | 6,8 % | même contenu sous deux formes |
| `horaires`, `horaires_jour` | chaîne contenant du JSON, chaîne | 13,1 % et 32,1 % | |
| `horaires_automate_24_24` | chaîne | 0,0 % | |

Total : 47 champs observés, 47 annoncés par les métadonnées (**mesuré** et **documenté**). Les métadonnées n'ont pas de description de champ (vide pour `services`, `rupture`, etc.) : la signification des champs est **non documentée**.

Flux quotidien : 26 champs (**documenté** par les métadonnées) dont `id`, `com_arm_code`, `com_arm_name`, `epci_code`, `epci_name`, `dep_code`, `reg_code`, `prix_maj`, `prix_id`, `prix_nom`, `prix_valeur`, `rupture_nom`, `rupture_debut`, `rupture_fin`, `fermeture`. Le taux de vide de `rupture_fin` et de `fermeture` n'est **pas mesuré**.

### La rupture est-elle dans le flux instantané v2 ? Oui

- Début de rupture : par carburant (`<carburant>_rupture_debut`, `_rupture_type`) et dans la chaîne JSON `rupture` (`@debut`, `@type`). **Mesuré**.
- Fin de rupture : la clé `@fin` existe dans chacune des 25 350 entrées du flux du 2026-11-05, et **vaut toujours vide** (0 renseignée, **mesuré**). Dans l'historique stocké (5 535 paires de versions consécutives), aucune entrée n'a jamais eu de `@fin` renseigné (**mesuré**).
- Correction d'une mention antérieure : « aucun champ de fin » était une lecture incomplète des métadonnées. Le champ existe dans la structure JSON, il n'est simplement jamais rempli.
- Ce que devient une rupture terminée : l'entrée **disparaît** de la liste. L'historique stocké compte 4 404 disparitions (**mesuré**), dont 4 275 de type `temporaire`, 123 `definitive`, 6 sans type.
- Conséquence pour une durée de rupture : le début vient de la source (`@debut`), la fin vient de nous (première version sans l'entrée). La fin est encadrée par deux versions successives, donc par l'intervalle de collecte, et **censurée** par les coupures de la collecte : si la rupture se termine pendant un arrêt, la fin est inconnue ou tardive.
- Une disparition n'est pas toujours une fin : sur les 4 404, 46,0 % sont des fins probables (la station n'a plus d'entrée pour ce carburant), 30,1 % sont remplacées dans la version suivante par une entrée du même carburant avec un `@debut` plus récent, 23,9 % réapparaissent plus tard avec un autre `@debut`. Aucune n'a un `@debut` plus ancien, donc rien n'indique une correction de la source. **Au total, 54 % des disparitions (30,1 % + 23,9 %) sont suivies, immédiatement ou plus tard, d'une nouvelle entrée du même carburant avec un `@debut` plus récent : une disparition n'est donc pas une fin de rupture dans plus d'un cas sur deux. La définition de la durée est à revoir en J7.** Ce que signifient ces réapparitions (succession de ruptures ou nouvelle déclaration d'une même rupture) est **non vérifié**.
- **Dans le flux annuel, `fin` est renseigné.** Sur l'extrait lu (M4b, 125 stations de 2026, 110 de 2025), 2 026 éléments `<rupture>` pour 2026 dont 1 689 avec une fin (83,4 %) et 351 pour 2025 dont 83 avec une fin (23,6 %), types : 2026 1 705 temporaires et 279 définitives, 2025 94 temporaires et 215 définitives (**mesuré sur l'extrait**, rejeu `make mesure-carburants M=m4b`). La part plus basse de 2025 reste **non expliquée** (hypothèse, non vérifiée : beaucoup de ruptures de 2025 sont définitives et restent ouvertes). Le flux annuel est donc une source possible de **fins de rupture** que l'instantané ne donne jamais. Extension au fichier entier, fuseau et cohérence avec nos `@debut` : **non vérifiés**.
- Par type : 76 % des 25 350 entrées du flux sont `definitive` (19 396), 4 921 `temporaire`, 1 033 sans type. **Mesuré**. Une rupture définitive correspond à un produit retiré de la station. Elle ne se mélange pas aux temporaires pour calculer une durée : la décision est pour J7.

## 5. Volumétrie

- Instantané : 9 826 stations dans le flux du 2026-11-05 (**mesuré**), environ 31 000 lignes de prix par lecture (30 909 à 30 955 aux passages du 05/11, **mesuré** par `passage_flux`). Zip roulez-eco : 981 639 octets (**mesuré**, HEAD).
- Quotidien : 75 645 lignes pour 9 950 identifiants de station distincts (**mesuré**, M3). Zip `jour` : 1 132 882 octets (**mesuré**, HEAD).
- Zip annuel : 31 744 821 octets pour `annee` et `annee/2026`, 31 747 153 octets pour `annee/2025` (**mesuré**, HEAD). Chaque zip contient un seul fichier XML : 359 376 940 octets décompressés pour 2026, 386 456 970 pour 2025 (**mesuré**, en-tête du zip, lu dans les 256 premiers Ko). Les deux zips sont donc des fichiers différents (dates, contenus et tailles décompressées distincts, début de flux différent) et la quasi-égalité des tailles comprimées (écart de 2 332 octets) est une **coïncidence apparente**, sans cause **vérifiée** (pas de téléchargement complet, pas de somme de contrôle comparée : le serveur ne renvoie pas d'ETag).
- Notre tampon Postgres : lignes, octets, croissance et extrapolation sont dans M8 (section 10), à une date donnée.

## 6. Historique

- **Quotidien : il ne se comporte pas comme un journal de changements.** Les dates `prix_maj` vont de 2009-04-17 à 2026-11-04 23:59 : 60 316 lignes sur les 7 derniers jours, 11 637 plus anciennes, 3 692 sans `prix_maj` (**mesuré**, M4). Une répartition de dates aussi étalée, pour 75 645 lignes seulement, indique un état courant à J-1 (dernier prix connu par station), pas une ligne par changement. **Vérifié sur le zip `jour` de roulez-eco** (M4c, fichier `PrixCarburants_quotidien_20261104.xml`, 1 132 882 octets, 9 950 stations, 30 554 prix, 27 636 ruptures) : 30 554 couples (station, carburant) distincts, **aucun couple avec plus d'un enregistrement** : un prix par couple, c'est un état courant, pas un journal (rejeu : `make mesure-carburants M=m4c`). La correspondance exacte entre ce zip et le jeu `prix-carburants-quotidien` du portail (mêmes 9 950 stations) est **non vérifiée**. Il ne permet donc pas de reconstituer les changements d'une période où notre collecte était arrêtée.
- **Flux annuel** (`donnees.roulez-eco.fr/opendata/annee`, `annee/2026`, `annee/2025`) : réponse HTTP 200 aux requêtes HEAD, zip, dernière modification de `annee` et `annee/2026` le 2026-11-04 23:38:48 GMT (**mesuré**, une seule observation, la fréquence de mise à jour n'est pas établie). Les chemins ne sont pas annoncés par la page d'index, qui répond 404 (**mesuré**) : leur existence est mesurée, leur statut de source officielle est **non vérifié**.
- **Flux annuel, lecture partielle (M4, 256 premiers Ko de chaque zip, une requête chacun, aucun téléchargement complet).**
  - Le serveur **ignore `Range`** : il répond 200 avec le fichier depuis le début, sans `Accept-Ranges`, `Content-Range` ni `ETag`. Je n'ai donc pas pu lire le répertoire central en fin de zip. J'ai lu 262 144 octets du début puis fermé la connexion. En-tête de zip (méthode deflate, un seul membre) : `PrixCarburants_annuel_2026.xml` et `PrixCarburants_annuel_2025.xml`. **Mesuré**.
  - Format : XML en ISO-8859-1, `<pdv id latitude longitude cp pop>` avec `adresse`, `ville`, `horaires`, `services`, des éléments `<prix nom id maj valeur/>`, `<rupture id nom debut fin type/>` et `<fermeture/>`. Latitude et longitude sont en entiers (4620100 pour 46,20100). **Mesuré** sur les 2,9 Mo décompressés du début (125 stations de 2026, 110 de 2025, ordre croissant d'identifiant).
  - **Granularité : un enregistrement `<prix>` par mise à jour datée, pas un prix par station et carburant.** Sur la première station de 2026, 209 enregistrements Gazole entre 2026-01-02 et 2026-11-04, dont 65 (31 %) ont la même valeur que le précédent. Sur tout l'extrait, 47,1 % des enregistrements de 2026 (17 358 sur 36 826 paires) et 42,1 % de ceux de 2025 (17 737 sur 42 172) répètent la valeur précédente du même carburant et de la même station (rejeu : `make mesure-carburants M=m4b`). Les dates `maj` de l'extrait vont de 2026-01-01 00:00 à 2026-11-04 21:59. Pour 2025 : de 2025-01-01 à 2025-12-31. **Mesuré sur l'extrait seulement** (125 stations sur environ 10 000) ; l'extension au fichier entier est **non vérifiée**.
  - Le fichier 2026 va donc jusqu'à J-1 : il peut combler nos trous au niveau de la mise à jour, pas seulement du jour. Une mise à jour à valeur identique est un enregistrement, donc un « changement de prix » exige de comparer à la valeur précédente.
  - Fuseau de `maj` dans le flux annuel (heure de Paris comme l'instantané ?), correspondance exacte avec les `maj` de notre tampon, licence, délai de publication, présence des stations disparues : **non vérifiés**.
  - Poids d'un rattrapage : 31,7 Mo à télécharger et 359 Mo de XML à parcourir pour 2026 (**mesuré**, en-tête). Un téléchargement complet n'a pas été fait, aucune mesure du temps de téléchargement.
- Notre collecte démarre le 2026-10-23 09:04:23 UTC. **Le flux annuel est un candidat de backfill** (reconstituer la période antérieure et les trous) : la décision est pour J4 ou J5, **hors périmètre de J3**. Non vérifiés : le fuseau de `maj`, la licence, la complétude (stations, mises à jour, ruptures) au-delà de l'extrait. **Le XML de 359 Mo se parse en flux (lecture incrémentale), jamais en mémoire dans le conteneur qui collecte** (voir la mémoire du conteneur airflow, déjà proche de 5 Gio le 05/11).
- Passages séparés du précédent de plus de 20 minutes (heure UTC ; lignes = lignes de prix insérées ; **mesuré**, M2). Le critère est unique et vaut pour M2, M5 et M6 :

| Passage | Écart avec le précédent | Lignes | Plage de `maj` |
|---|---|---|---|
| 10-23 09:04:23 | premier passage | 31 509 | 07-28 à 10-23 |
| 10-23 09:32:06 | 26 min | 465 | 10-23 08:50 à 09:09 |
| 10-29 15:49:35 | 6 j 4 h 49 | 21 683 | 10-23 10:42 à 10-29 15:19 |
| 10-30 09:41:44 | 17 h 11 | 9 661 | 10-29 16:09 à 10-30 09:29 |
| 10-30 15:17:51 | 46 min | 247 | 10-30 14:10 à 14:49 |
| 10-30 15:45:03 | 27 min | 178 | 10-30 14:50 à 15:19 |
| 11-05 08:50:29 | 5 j 17 h | 25 067 | 10-30 15:22 à 11-05 08:29 |

Le passage du 30/10 14:31:50 a 28 min d'écart avec le passage précédent de `passage_flux`, mais le passage de 14:15:41 existe dans `prix_flux` (126 lignes, aucune version d'état, pas de ligne de journal) : le run de 14:15 a échoué après le chargement des prix. L'écart réel est de 16 minutes et ce passage n'est pas écarté.

**`passage_flux` n'est pas exhaustif.** Le journal est écrit en fin de tâche et son écriture est volontairement non bloquante : un échec d'écriture ne fait pas échouer le run. Cas du 14:15 : le journal de la tâche `charger` (tentative 1) s'arrête sur `Prix : {'lot': 31188, 'inserees': 126, ...}` à 14:15:59, puis **`Process terminated by signal`, signal -9 (SIGKILL) à 14:16:28**. Aucune tentative d'écriture du journal n'a échoué : le processus a été tué avant d'y arriver. Il n'y a qu'une tentative (pas de `attempt=2`). La cause du SIGKILL (mémoire du conteneur, arrêt externe) est **non vérifiée**. Conséquence : la couverture se calcule sur **l'union des `collecte_ts` de `prix_flux` et de `passage_flux`**, pas sur `passage_flux` seul (c'est ce que fait M2).

## 7. Pièges

1. **`maj` n'est pas en UTC.** La source étiquette les dates `+00:00` alors que ce sont des heures de Paris. Nous lisons l'heure murale en Europe/Paris puis convertissons en UTC (l'heure répétée de la nuit du 25 octobre est lue comme sa première occurrence). `maj` est l'heure source, `collecte_ts` l'heure de notre passage : ne jamais les mélanger.
2. **`services`, `rupture`, `horaires` et `prix` sont des chaînes qui contiennent du JSON**, pas des listes. Le hash d'état parse toute chaîne commençant par `{` ou `[` qui donne une liste ou un objet, puis trie les listes (`ingestion/carburants.py`).
3. **L'ordre de `services` a changé à la source**, une fois, entre le 2026-10-30 15:32 et le 2026-11-05 08:50 UTC (date exacte inconnue). Sur les lots conservés : 8 021 stations sur 9 813 avec un `services` différent, dont 16 seulement avec un contenu différent (**mesuré**). Stable depuis sur quatre lots. La cause n'est pas documentée. Le garde-fou à 30 % a bloqué l'écriture des états (8 269 stations sur 9 816 vues comme changées), correctement.
4. **Recalcul des hash du 2026-11-05 10:00:39 UTC** : 13 460 lignes de `station_etat_flux` sur 13 474 ont reçu un nouveau `etat_hash` (UPDATE, exception à l'append-only), `etat` inchangé, 0 version consécutive devenue identique. Commits `ba7c08d` et `54aec9a`.
5. **Pic de 1 889 états le 2026-11-05 10:15** (1 898 versions avec 9 nouvelles stations) : changements accumulés depuis le 30/10 15:45, dont les dates réelles sont inconnues. Les versions portent le `collecte_ts` du 10:15. À écarter de tout taux de changement.
6. **Trous de collecte.** Arrêt du 29/10 (6 j) à la reprise, arrêt du 30/10 15:45 au 05/11 08:50. Avant le 2026-10-30 13:02, un passage sans insertion ne laisse aucune trace (le journal `passage_flux` n'existait pas) : les écarts avant cette date peuvent cacher un passage sans insertion.
7. **Un lot à zéro insertion est un comportement normal** (31 190 lignes déjà vues le 29/10 09:45, aucun conflit). Les barres de progression de Great Expectations sont journalisées au niveau `error` : normal aussi.
8. **Run en échec après chargement des prix** (30/10 14:15) : les prix sont écrits, les états et le journal pas. Les états sont rattrapés au passage suivant. Le processus a été tué par un SIGKILL (signal -9) à 14:16:28, cause **non vérifiée** (voir section 6).
9. **Rupture** : `@fin` jamais renseigné, une rupture terminée disparaît (voir section 4). Durée : début source, fin observée, censure par les trous.
10. **Le quotidien ne fournit pas d'historique de changements** (voir section 6).

## 8. Requêtes et commandes d'échantillon

```text
make mesure-carburants M=m1   champs, types, taux de vides, ruptures, disparitions
make mesure-carburants M=m2   taux de changement avec et sans les lots de rattrapage
make mesure-carburants M=m3   code commune, couverture des identifiants
make mesure-carburants M=m4   en-têtes HTTP du portail roulez-eco, étendue du quotidien
make mesure-carburants M=m4b  début du flux annuel (au plus 256 Ko lus), granularité
make mesure-carburants M=m4c  unicité du zip jour (état courant ou journal)
make mesure-carburants M=m5   latence collecte_ts moins maj, avec et sans rattrapage
make mesure-carburants M=m8   volumétrie du tampon, croissance extrapolée, poids d'un téléchargement
make test                     tests des fonctions pures (ingestion/tests/test_mesures_carburants.py)
```

Échantillon du quotidien (une ligne avec code commune) :
`https://data.economie.gouv.fr/api/explore/v2.1/catalog/datasets/prix-carburants-quotidien/records?limit=1&where=com_arm_code%20is%20not%20null`
donne par exemple `com_arm_code = "01344"`, `com_arm_name = "Saint-Denis-lès-Bourg"`, `dep_code = "01"`.

## 9. Décision d'ingestion

- Cadence : toutes les 15 minutes (DAG `collecte_flux`), sans rattrapage (`catchup=False`).
- Clé de dédoublonnage des prix : (`station_id`, `carburant`, `maj`), `ON CONFLICT DO NOTHING`. Les états : une version par station quand le hash canonique change, garde-fou à 30 % de stations changées par passage.
- Tampon : base Postgres `collecte` (`prix_flux`, `station_etat_flux`, `passage_flux`), puis chargement vers `carburant_bronze`.
- Contrôles : Great Expectations valide chaque lot avant chargement.
- Code commune : absent de l'instantané, à prendre dans le quotidien (`com_arm_code`) ou par jointure spatiale sur les contours en J6 (voir section 10).
- La collecte s'arrête à la fin du projet (pas d'hôte 24 h sur 24) : `passage_flux` mesure la couverture.

## 10. Résultats des mesures

### M3 : code commune (mesuré, `make mesure-carburants M=m3`)

- Le flux instantané n'a **aucun code commune** : il a `cp`, `ville`, `code_departement`, `code_region` (**mesuré**, M1).
- Le quotidien a `com_arm_code`, renseigné pour 99,9 % de ses identifiants de station (9 939 sur 9 950). Aucun identifiant n'a plusieurs codes différents. Aucun code n'a une forme inattendue (cinq chiffres, ou 2A et 2B suivis de trois chiffres).
- Couverture : 9 821 identifiants communs aux deux flux, soit 99,9 % des 9 826 de l'instantané. 5 stations de l'instantané sont absentes du quotidien, 129 stations du quotidien sont absentes de l'instantané. 9 817 stations de l'instantané (99,9 %) obtiennent un `com_arm_code` via le quotidien.
- Concordance avec la jointure spatiale (J6) : **non mesurée**, les contours des communes sont reportés en J6.
- `com_arm_code` remplace peut-être la jointure spatiale de J6 : c'est une **décision à prendre en J6**, pas encore prise. Points à vérifier avant : les 11 stations du quotidien sans code (et les 5 de l'instantané absentes du quotidien), la forme du code pour les arrondissements de Paris, Lyon et Marseille (le nom `com_arm_*` suggère un code commune ou arrondissement : **non vérifié** face au COG), et le fait que la valeur reflète la commune actuelle ou celle de la déclaration (**non vérifié**).

### M2 : taux de changement (mesuré, `make mesure-carburants M=m2`)

Mesuré à 10:40 UTC. Critère de rattrapage : voir section 6 (7 lots sur 44 passages visibles à cette heure ; M8 et M5, mesurés à 12:30 UTC, comptent plus de passages). Pour les états, on écarte en plus le premier chargement (9 816 versions le 10-23 09:45:23) et le pic du 11-05 10:15 (1 898 versions).

Prix insérés par passage (un passage = 15 minutes) :

| Population | Avec rattrapage | Sans rattrapage |
|---|---|---|
| Tous les passages visibles (le biais vers le haut avant le 30/10 13:02 vient des passages sans insertion invisibles) | n=44, moyenne 2 182, médiane 170,5, p95 21 683 | n=37, moyenne 194,6, médiane 139, p95 415 |
| Passages journalisés (fiables) | n=16, moyenne 1 786, médiane 235, p95 591 | n=13, moyenne 237, médiane 226, p95 541 |

Part des lignes lues qui sont réellement nouvelles, sans rattrapage, passages journalisés : médiane 0,73 %, p95 1,75 % (n=13).

Versions d'état par passage :

| Population | Avec rattrapage | Sans rattrapage |
|---|---|---|
| Passages ayant au moins une version (n=33 puis n=27) | moyenne 467, médiane 45, p95 1 697 | moyenne 37,4, médiane 38, p95 61 |
| Passages journalisés avec états chargés, zéros inclus (n=9 puis n=6) | moyenne 259, médiane 58, p95 1 889 | moyenne 44,8, médiane 49, p95 61 |

Lots écartés pour les états (versions écrites) : 10-23 09:45:23 (9 816), 10-29 15:49:35 (1 697), 10-30 09:41:44 (832), 10-30 15:17:51 (103), 10-30 15:45:03 (69), 11-05 10:15:02 (1 898).

Les effectifs sont petits (13 passages journalisés fiables sans rattrapage, 6 pour les états) : les médianes sont des ordres de grandeur, pas des taux stables.

### M4 : historique (mesuré, `make mesure-carburants M=m4`, `M=m4b` et `M=m4c`)

Voir la section 6. Résumé : le quotidien est un état courant, pas un journal (un prix par station et carburant dans le zip `jour`, M4c). Le flux annuel est un journal de mises à jour datées (XML, un `<prix>` par `maj`, 42 à 47 % de valeurs répétées sur l'extrait), mais seuls 256 Ko par zip ont été lus (le serveur ignore `Range`), donc l'extension au fichier complet, le fuseau de `maj` et la licence sont **non vérifiés**. Les tailles quasi égales des zips 2025 et 2026 sont une coïncidence apparente (tailles décompressées différentes).

### M5 : latence de publication

**Mesuré** (`make mesure-carburants M=m5`, 2026-11-05 12:30 UTC). Latence d'une ligne = `collecte_ts` de notre premier passage qui la voit moins sa `maj` (convertie de l'heure de Paris en UTC). En minutes :

| Population | n | min | médiane | p95 | max | négatives |
|---|---|---|---|---|---|---|
| AVEC les 7 lots de rattrapage | 97 085 | 12,0 | 1 019,6 | 18 677,2 | 650 572,4 | 0 |
| SANS les lots de rattrapage | 8 275 | 15,2 | 30,0 | 39,1 | 42,6 | 0 |
| SANS, `maj` en heure d'été (UTC+2) | 1 206 | 16,6 | 29,0 | 40,1 | | 0 |
| SANS, `maj` en heure d'hiver (UTC+1) | 7 069 | 15,2 | 30,1 | 39,1 | | 0 |

- Les chiffres AVEC ne décrivent pas la source : ils mesurent nos trous de collecte.
- Sans rattrapage, la latence va de 15 à 43 minutes, médiane 30. Notre sondage toutes les 15 minutes y ajoute 0 à 15 minutes (7,5 en moyenne). La latence mesurée est comprise entre 15,2 et 42,6 minutes, avec un décalage minimal d'environ 15 minutes : aucune ligne n'apparaît moins de 15 minutes après sa `maj`. C'est une **interprétation**, pas une mesure : un décalage minimal d'environ 15 minutes est compatible avec un délai de publication ou de déclaration de la `maj`, dont la cause est **non vérifiée**. Notre intervalle de 15 minutes ne l'explique pas à lui seul, puisqu'il ajoute entre 0 et 15 minutes et non un minimum.
- Aucune latence négative, ni en heure d'été ni en heure d'hiver : la conversion Paris vers UTC est cohérente (une lecture de `maj` comme UTC donnerait -55 minutes en hiver, ce que le test `test_une_conversion_sans_decalage_donnerait_une_latence_negative_en_hiver` illustre, et `test_latence_jamais_negative_apres_conversion_paris_en_ete_et_en_hiver` le fixe pour les deux régimes). La période d'été ne compte que 1 206 lignes (collecte du 23/10), à lire avec prudence.
- La latence réelle des lignes déjà dans l'historique avant notre premier passage est **non mesurable** par notre collecte.

### M6, M7 : volatilité par heure de `maj`, fuseau

**Non mesurés** (décision du 2026-11-05, faute de temps). Rejeu prévu : à écrire dans `ingestion/mesures_carburants.py` (`m6` : nombre de lignes insérées par heure de `maj`, hors rattrapage ; `m7` : vérification des `maj` autour du 25 octobre 2026 03:00). Tout ce que la fiche dit de la volatilité horaire et de l'heure répétée du 25/10 est donc **non vérifié**.

### M8 : volumétrie du tampon

**Mesuré** (`make mesure-carburants M=m8`, 2026-11-05 12:30 UTC), tampon Postgres `collecte` :

| Table | Lignes | Octets avec index | Octets table seule | Octets par ligne |
|---|---|---|---|---|
| `prix_flux` | 97 085 | 12 541 952 | 6 635 520 | 129 |
| `station_etat_flux` | 15 672 | 41 115 648 | 40 165 376 | 2 624 |
| `passage_flux` | 23 | 24 576 | 8 192 | 1 069 |

- Aucun jour n'est complet dans l'historique (collecte du 23/10 au 29/10 interrompue, puis du 30/10 au 05/11). La croissance par jour est donc **extrapolée par passage** (96 passages par jour si la collecte tourne 24 h sur 24), hors lots de rattrapage :
  - Prix : médiane 139,5 lignes par passage (n=44 passages avec insertion hors lots écartés **à 12:30 UTC** ; M2 a été mesuré à 10:40 UTC avec n=37 passages hors rattrapage, les 7 passages de différence sont ceux de 10:45 à 12:30. Les passages sans insertion avant le 30/10 sont invisibles, la médiane est donc un majorant) soit environ 13 400 lignes et 1,7 Mo par jour, 156 Mo pour 90 jours.
  - États : médiane 39 versions par passage ayant au moins une version (n=36) soit environ 3 700 versions et 9,8 Mo par jour, 882 Mo pour 90 jours. Les états dominent la taille : 2,6 Ko par version (JSONB complet), contre 129 octets par ligne de prix.
- Poids d'un téléchargement du flux instantané : environ 4,7 Mo sur le réseau (gzip) et 29,3 Mo de JSON décompressé (**mesuré**, une lecture). Le zip `instantané` de roulez-eco pèse 0,98 Mo (HEAD), ce qui en ferait une source plus légère, avec un format XML différent : **non évalué**.
- Les extrapolations supposent une collecte continue et un taux de changement stable (hypothèses **non vérifiées** sur plus de quelques jours).
