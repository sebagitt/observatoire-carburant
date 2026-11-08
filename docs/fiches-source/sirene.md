# Fiche-source : base Sirene des entreprises et de leurs établissements (INSEE), sur documentation seule

Brouillon du 2026-11-08. **Aucune donnée de stock n'a été téléchargée** : tout ce qui suit vient de la documentation. Chaque fait porte une étiquette : **documenté** (avec la source), **mesuré** (sur un petit fichier téléchargé ou les métadonnées du jeu) ou **non vérifié / non indiqué**. Les chargeurs, la suite Great Expectations et les mesures de contenu sont prévus plus tard (J8, si B-lite).

## 1. Identité

- Nom : Base Sirene des entreprises et de leurs établissements (SIREN, SIRET). Éditeur : INSEE, diffusée sur data.gouv.fr (`https://www.data.gouv.fr/fr/datasets/base-sirene-des-entreprises-et-de-leurs-etablissements-siren-siret/`).
- Fichiers de stock mensuels (**documenté**, métadonnées du jeu lues le 2026-11-08 via l'API de data.gouv.fr) : `StockUniteLegale`, `StockEtablissement`, `StockEtablissementHistorique`, `StockUniteLegaleHistorique`, `StockEtablissementLiensSuccession`, `StockDoublons`, chacun en zip et en parquet, version « 01 novembre 2026 ».
- Documentation des fichiers lue (3 fichiers, 250 820 octets au total, **mesuré**) : `description-fichier-stocketablissement-311.pdf` (127 863 octets), `description-fichier-stocketablissementhistorique-311.pdf` (118 082 octets), `liste-csv-des-variables-du-dessin-du-fichier-stocketablissement-311.csv` (4 875 octets). Les PDF portent la mention « Version du 15 janvier 2026 » ; le CSV est daté du 13 janvier 2026 sur data.gouv.fr. La documentation de l'API Sirene n'a pas pu être lue (voir le piège 5).
- Date de consultation : 2026-11-08.

## 2. Licence

- Le jeu est publié sous « lov2 », c'est-à-dire la Licence Ouverte 2.0 (Etalab), d'après le champ de licence renvoyé par l'API de data.gouv.fr (**mesuré**, 2026-11-08) ; un sous-agent a lu la même mention sur la page du jeu (**documenté**).
- Conditions : mentionner la source (« Source : Insee ») et la date de mise à jour, comme pour les autres données de l'INSEE (**documenté**, page « Mentions légales et crédits » du 2026-04-13).

## 3. Fraîcheur

- Mise à jour **mensuelle** : chaque fichier reflète le répertoire au dernier jour du mois précédent (**documenté**, page du jeu lue par un sous-agent). Des mises à jour plus fréquentes passent par l'API Sirene (**documenté**).
- Jeu modifié pour la dernière fois le 2026-11-05 06:39:40 UTC ; `StockEtablissement` le 2026-11-05, les autres stocks le 2026-11-03 (**mesuré**, métadonnées data.gouv.fr).
- Les fichiers de documentation datent du 2026-01-13 ou du 2026-01-29 sur data.gouv.fr et du 2026-10-19 sur le serveur de fichiers (`last-modified`, **mesuré**) : écart non expliqué.

## 4. Schéma (variables du stock des établissements)

Source : `liste-csv-des-variables-du-dessin-du-fichier-stocketablissement-311.csv`, 54 variables numérotées de 1 à 54 (colonnes : nom, libellé, longueur, type, ordre) et description PDF du 15 janvier 2026 (**mesuré**, lecture des fichiers). Les types des valeurs ne sont pas mesurés sur des données.

| Besoin du projet | Variable | Longueur | Type annoncé | Remarque |
|---|---|---|---|---|
| clé | `siret` (`siren` 9, `nic` 5) | 14 | Texte | établissement |
| activité principale | `activitePrincipaleEtablissement` | 6 | Liste de codes | un code de 6 caractères tient le format `47.30Z` ; **format non indiqué** |
| **version de la nomenclature** | `nomenclatureActivitePrincipaleEtablissement` | 8 | Liste de codes | « Nomenclature d'activité de la variable activitePrincipaleEtablissement » ; **valeurs non indiquées** dans ces fichiers |
| activité en NAF 2025 | `activitePrincipaleNAF25Etablissement` | 6 | Liste de codes | « Activité principale de l'établissement en nomenclature NAF25 » ; présente dans le stock courant (ordre 54), **absente de la liste du fichier historique** |
| commune | `codeCommuneEtablissement` | 5 | Liste de codes | « Code commune de l'établissement » ; à rapprocher du COG, correspondance **non mesurée** |
| adresse | `numeroVoieEtablissement`, `typeVoieEtablissement`, `libelleVoieEtablissement`, `codePostalEtablissement`, `libelleCommuneEtablissement`, `identifiantAdresseEtablissement` | 9, 30, 100, 9, 100, 15 | Texte | |
| coordonnées | `coordonneeLambertAbscisseEtablissement`, `coordonneeLambertOrdonneeEtablissement` | 18, 18 | Texte | « coordonnées Lambert de l'adresse » ; **système de projection non indiqué** ; noms avec une espace finale dans le CSV |
| enseigne | `enseigne1Etablissement`, `enseigne2Etablissement`, `enseigne3Etablissement`, `denominationUsuelleEtablissement` | 50, 50, 50, 100 | Texte | |
| état | `etatAdministratifEtablissement` | 1 | Liste de codes | **valeurs non indiquées** dans ces fichiers |
| dates | `dateCreationEtablissement` (10), `dateDebut` (10, « début d'une période d'historique »), `dateDernierTraitementEtablissement` (23) | | Date | |
| diffusion | `statutDiffusionEtablissement` | 1 | Liste de codes | diffusion partielle expliquée dans la documentation de l'API, **non lue** |

Taux de nulls : **non mesuré** (aucune donnée téléchargée).

## 5. Volumétrie

Tailles exactes annoncées pour la version « 01 novembre 2026 » (**mesuré**, champ `filesize` des ressources data.gouv.fr, en octets) :

| Fichier | Zip | Parquet |
|---|---|---|
| StockUniteLegale | 984 970 049 | 717 824 037 |
| StockEtablissement | 2 892 689 384 | 2 231 588 933 |
| StockEtablissementHistorique | 1 250 810 280 | 882 032 833 |
| StockUniteLegaleHistorique | 1 271 584 341 | 867 456 396 |
| StockEtablissementLiensSuccession | 121 154 823 | 120 218 496 |
| StockDoublons | 1 117 602 | 1 111 886 |

Nombre d'établissements et d'unités légales : **non indiqué** dans les pages et métadonnées lues. Le stock des établissements pèse près de 2,9 Go en zip : il ne se charge pas en entier dans le conteneur de collecte (le conteneur airflow occupe déjà environ 2 Gio sur les 7,56 Gio de la machine virtuelle de Docker), il faudrait un filtre à la lecture ou un chargement dans Databricks (le format parquet est proposé).

## 6. Historique

- Le stock courant (`StockEtablissement`) donne les « valeurs courantes » ; l'historique des valeurs est dans `StockEtablissementHistorique` (« valeurs historisées »), avec une période par ligne (**documenté**, titres des descriptions).
- Variables de l'historique des établissements (**mesuré**, lecture du PDF) : `siret`, `siren`, `nic`, **`dateDebut`**, **`dateFin`**, `etatAdministratifEtablissement`, `changementEtatAdministratifEtablissement`, `activitePrincipaleEtablissement`, `nomenclatureActivitePrincipaleEtablissement`, `changementActivitePrincipaleEtablissement`, `caractereEmployeurEtablissement`, `changementCaractereEmployeurEtablissement`, `enseigne1Etablissement` à `enseigne3Etablissement`, `denominationUsuelleEtablissement` et leurs variables `changement…`.
- **Où est la date de cessation d'un établissement** : il n'y a **aucune variable nommée « date de cessation »** dans les deux fichiers lus. L'information se déduit de l'historique : `dateFin` d'une période (fin de validité de la valeur) et `etatAdministratifEtablissement` avec son indicateur `changementEtatAdministratifEtablissement` (**déduction à partir des noms de variables, non vérifiée**). Dans le stock courant, seul `dateDebut` (début de la période courante) et `etatAdministratifEtablissement` existent. Les valeurs du code d'état (par exemple actif ou fermé) sont **non indiquées** dans ces fichiers : elles sont dans la documentation de l'API, non lue.
- Une cessation de l'unité légale est portée par `StockUniteLegale` et `StockUniteLegaleHistorique` : **non lus**.

## 7. Pièges

1. **NAF rév. 2 puis NAF 2025.** Un sous-agent a lu sur la page de l'INSEE sur la NAF (mise à jour le 2025-07-30) que la NAF rév. 2 est en vigueur jusqu'à fin 2026 et que la NAF 2025 s'applique en janvier 2027 (**documenté**, sous-agent, **non revérifié** dans les fichiers). Le stock courant contient déjà la colonne `activitePrincipaleNAF25Etablissement` (**mesuré**, liste des variables) : un filtre sur l'activité doit lire `nomenclatureActivitePrincipaleEtablissement` pour savoir de quelle nomenclature vient `activitePrincipaleEtablissement`. Si la colonne NAF25 est remplie pour tous les établissements : **non vérifié**.
2. **Intitulés de la NAF rév. 2** (**documenté**, pages de l'INSEE sur la nomenclature, mises à jour le 2025-12-19, résumé non revérifié à la source) : 47.30Z « Commerce de détail de carburants en magasin spécialisé » ; 47.11E « Magasins multi-commerces » ; 47.11F « Hypermarchés ». Lequel des codes décrit une station-service du flux carburant : décision non prise.
3. **Diffusion partielle** : l'existence d'un statut de diffusion est documentée ; les valeurs masquées et les variables concernées sont décrites dans l'API, **non lues**.
4. **Dessin de fichier** : le CSV des variables annonce une longueur `null` pour cinq variables d'adresse secondaire (`libelleCommune2Etablissement`, `libelleCommuneEtranger2Etablissement`, `codeCommune2Etablissement`, `codePaysEtranger2Etablissement`, `libellePaysEtranger2Etablissement`, **mesuré**) : les longueurs affichées ne sont pas toutes fiables.
5. **API Sirene** : l'existence est documentée sur le portail `portail-api.insee.fr`, mais la page n'a pas pu être rendue (version, authentification et quotas **non indiqués**). Le sous-agent n'a pas pu lire `www.sirene.fr`.
6. Le format des fichiers de documentation sur data.gouv.fr (PDF de 3 à 4 pages) ne décrit pas les variables : il renvoie à la documentation de l'API pour leur signification.

## 8. Requêtes et commandes d'échantillon

```text
Métadonnées des ressources du jeu (aucun fichier de stock) :
curl -s https://www.data.gouv.fr/api/1/datasets/base-sirene-des-entreprises-et-de-leurs-etablissements-siren-siret/
  -> 24 ressources : 12 fichiers de stock (zip et parquet), 12 documents (PDF et CSV) ; licence « lov2 ».
```

Aucune requête d'échantillon sur les données : non faite.

## 9. Décision d'ingestion (prévue, non exécutée)

- Données : en J8 si B-lite, **hors périmètre de J3** (**décision**).
- Source retenue pressentie : fichiers parquet filtrés à la lecture, plutôt que les zips, pour un chargement mensuel sans clé d'API : à confirmer, **aucune mesure**. B-lite exige **deux fichiers** : `StockEtablissement` (2,2 Go en parquet) pour les créations et `StockEtablissementHistorique` (882 Mo en parquet) pour les cessations, la date de cessation se déduisant de l'historique (voir la section 6).
- Filtre sur l'activité dans un fichier de configuration (le choix 47.30Z, 47.11E ou 47.11F reste à décider) ; lecture de `nomenclatureActivitePrincipaleEtablissement` pour savoir de quelle nomenclature vient le code.
- Clé : `siret`. Rapprochement avec les stations par `codeCommuneEtablissement` et les coordonnées, à définir.
- Contrôles à écrire avec les données : unicité du `siret`, forme du code commune, valeurs du code d'état, forme du code d'activité, part de valeurs vides.
- Reste à lire avant d'écrire un chargeur : la documentation de l'API Sirene (valeurs de `etatAdministratifEtablissement`, `statutDiffusionEtablissement`, format de `activitePrincipaleEtablissement`).
