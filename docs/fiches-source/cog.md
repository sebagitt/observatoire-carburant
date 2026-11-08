# Fiche-source : Code officiel géographique (COG) 2026 de l'INSEE

Brouillon du 2026-11-08. Chaque résultat porte une étiquette : **mesuré** (commande de rejeu donnée), **documenté** (lu sur une page de l'INSEE) ou **non vérifié**.
Rejeu : `make charger-cog` (télécharge, valide avec Great Expectations, charge), `make mesure-carburants M=m9` (couverture des codes du flux quotidien). Code : `ingestion/referentiels.py`, `quality/gx/ref_cog_commune.py`.

## 1. Identité

- Nom : Code officiel géographique, millésime 2026 (situation au 1er janvier 2026, **documenté**).
- Éditeur : INSEE. Page : `https://www.insee.fr/fr/information/8740222`, dernière mise à jour affichée le 2026-02-24 (**documenté**).
- Fichiers utilisés : `v_commune_2026.csv`, `v_departement_2026.csv`, `v_region_2026.csv`, à l'adresse `https://www.insee.fr/fr/statistiques/fichier/8740222/<nom>`.
- Fichiers non utilisés pour l'instant : `v_mvt_commune_2026.csv` (événements sur les communes depuis 1943, **documenté**) et l'historique `v_commune_depuis_1943`. Décision du 2026-11-08 : pas de table `ref_cog_mouvement` pour l'instant.
- Date de consultation : 2026-11-08.

## 2. Licence

- La page « Mentions légales et crédits » (`https://www.insee.fr/fr/information/2008466`, mise à jour le 2026-04-13) indique : « Sauf mention contraire, les informations publiques diffusées sur ce site […] sont mises à disposition sous la Licence Ouverte / Open Licence version 2.0 (Etalab). » (**documenté**, lu le 2026-11-08 par un sous-agent, citation en deux extraits).
- Conditions : réutilisation libre, y compris commerciale, en mentionnant « Source : Insee », la date de dernière mise à jour des données lorsqu'elle est connue, et sans altérer le sens des informations (**documenté**).
- **Mention à reprendre dans le README du projet** : « Source : Insee, Code officiel géographique 2026 (mise à jour du 2026-02-24) ». C'est une condition de la licence. Le README n'est pas encore écrit (à faire avant la publication).
- La page du COG ne cite pas de licence propre : c'est la licence du site qui s'applique « sauf mention contraire », et aucune mention contraire n'a été trouvée (**documenté**).

## 3. Fraîcheur

- Un millésime par année civile, référencé au 1er janvier (**documenté**). Fréquence de publication des millésimes suivants : **non vérifiée**.
- Les requêtes `HEAD` sur les trois fichiers répondent 200 avec `Content-Type: text/csv` et `Content-Disposition`, **sans `Content-Length` ni `Last-Modified`** (**mesuré**, 2026-11-08) : la date de dernière modification du fichier n'est pas mesurable côté serveur.
- Empreintes sha256 des fichiers téléchargés le 2026-11-08 (identiques à deux téléchargements successifs, **mesuré**) : `7be1153de3a63df4…` (commune), `513900ca23edc682…` (département), `d903566d9c0d2633…` (région).

## 4. Schéma

Format mesuré le 2026-11-08 : séparateur virgule, tous les champs entre guillemets, fins de ligne CRLF, UTF-8 sans BOM, première ligne d'en-tête (**mesuré**). La page indique l'encodage UTF-8 mais pas le séparateur (**documenté**).

`v_commune_2026.csv` : 37 496 lignes de données, 12 colonnes, toutes lues en texte (zéros de tête conservés, `2A` et `2B` pour la Corse).

| Colonne | Signification (documenté) | Vide (mesuré) |
|---|---|---|
| `TYPECOM` | type de commune : `COM` Commune, `COMA` Commune associée, `COMD` Commune déléguée, `ARM` Arrondissement municipal | jamais |
| `COM` | code commune | jamais |
| `REG`, `DEP`, `CTCD`, `ARR`, `CAN` | région, département, collectivité territoriale à compétences départementales, arrondissement, canton | `COM` : `ARR` 17 sur 34 875, `CAN` 56 sur 34 875 ; `COMD` et `COMA` : toujours vides ; `ARM` : jamais vides |
| `TNCC`, `NCC`, `NCCENR`, `LIBELLE` | type de nom en clair, nom en majuscules, nom enrichi, libellé | jamais |
| `COMPARENT` | commune parente des arrondissements municipaux et des communes associées ou déléguées | `COM` : toujours vide ; `COMD`, `COMA`, `ARM` : jamais vide |

Clé : (`TYPECOM`, `COM`), unique (0 doublon, **mesuré**). Exemple (`COM`) : `01001`, région `84`, département `01`, `LIBELLE` « L'Abergement-Clémenciat ».

`v_departement_2026.csv` : 101 lignes, 7 colonnes (`DEP`, `REG`, `CHEFLIEU`, `TNCC`, `NCC`, `NCCENR`, `LIBELLE`), aucun vide. `v_region_2026.csv` : 18 lignes, 6 colonnes (`REG`, `CHEFLIEU`, `TNCC`, `NCC`, `NCCENR`, `LIBELLE`), aucun vide (**mesuré**).

## 5. Volumétrie

- `v_commune_2026.csv` 3 565 935 octets, `v_departement_2026.csv` 6 076 octets, `v_region_2026.csv` 1 245 octets (**mesuré**).
- Répartition par `TYPECOM` : 34 875 `COM`, 2 105 `COMD`, 471 `COMA`, 45 `ARM` (**mesuré**). Les 45 arrondissements se répartissent entre Paris (75056) pour 20, Lyon (69123) pour 9 et Marseille (13055) pour 16 (**mesuré**).
- Table `ref_cog_commune` : 11 Mo avec index pour 37 496 lignes (**mesuré**, `pg_total_relation_size`).
- Évolution : un nouveau millésime par an (**non vérifié**).

## 6. Historique

- Le millésime 2026 donne la situation au 1er janvier 2026 seulement. L'historique des communes (fusions, créations, suppressions) est dans `v_mvt_commune_2026.csv` et `v_commune_depuis_1943` (**documenté**), non chargés.
- Conséquence : un code commune d'une source plus ancienne qui a disparu par fusion n'aurait pas de correspondance dans `ref_cog_commune`. Sur le flux quotidien du 2026-11-08, aucun code n'est sans correspondance (voir section 8). Pour une source plus ancienne : **non vérifié**.

## 7. Pièges

1. **Le même code figure sous plusieurs types** : 584 codes `COM` apparaissent plusieurs fois dans le fichier (une commune nouvelle et sa commune déléguée portent le même code, **mesuré**). La clé est donc (`TYPECOM`, `COM`), jamais `COM` seul.
2. **Les communes déléguées et associées n'ont ni région, ni département, ni canton**. Seule leur commune parente (`COMPARENT`) permet de les rattacher (**mesuré**).
3. **Les arrondissements de Paris, Lyon et Marseille sont des lignes à part** (`ARM`), de parents 75056, 69123 et 13055. Les trois codes parents sont aussi des lignes `COM` du fichier (Paris, Lyon, Marseille, **mesuré**). Tous les codes de communes ont 5 chiffres ou `2A`/`2B` suivis de 3 chiffres, sans exception (**mesuré**).
4. **Mayotte** : la page annonce « Département-Région de Mayotte, code 976R » (**documenté**). Dans le fichier, `976R` apparaît dans la colonne `CTCD` des 17 communes de Mayotte, et le fichier des départements garde le code `976` (**mesuré**).
5. **Codes de département à trois chiffres** pour l'outre-mer (`971`, `972`, `973`, `974`, `976`) et `2A`, `2B` pour la Corse (**mesuré**).
6. **« Commune en vigueur » = `COM` est une lecture, non une règle documentée** : la page des modalités ne dit pas « en vigueur ». Les types `COMA`, `COMD` et `ARM` sont des entités infra-communales (**documenté**). Que `COM` désigne exactement les communes en vigueur au 1er janvier 2026 est **non vérifié**.
7. **Accents** : un terminal qui n'est pas en UTF-8 affiche mal les accents (constaté dans la console Windows). Les fichiers sont valides en UTF-8 (**mesuré**) ; lancer Python avec `PYTHONUTF8=1`.

## 8. Requêtes et commandes d'échantillon

```text
make charger-cog                                télécharge, valide (GX), remplace le millésime 2026 : 37 496, 101 et 18 lignes
make mesure-carburants M=m9                     couverture des com_arm_code du flux quotidien
```

```sql
SELECT typecom, count(*) FROM ref_cog_commune WHERE millesime = 2026 GROUP BY 1 ORDER BY 1;
-- ARM 45, COM 34875, COMA 471, COMD 2105
```

Couverture des codes commune du flux quotidien `prix-carburants-quotidien` (**mesuré**, M9, 2026-11-08) :
- 9 949 stations dans l'export, 11 sans code, 5 631 codes distincts.
- 5 596 codes sont des communes (`COM`) pour 9 807 stations, 35 codes sont des arrondissements (`ARM`) pour 131 stations, **0 code sans correspondance** dans `ref_cog_commune`, 0 code qui ne serait que commune déléguée ou associée.
- Paris : le code 75056 n'apparaît jamais, 13 codes d'arrondissement apparaissent (sur 20). Lyon : 69123 n'apparaît jamais, 7 codes d'arrondissement (sur 9). Marseille : 13055 n'apparaît jamais, 15 codes d'arrondissement (sur 16).
- Conclusion mesurée : `com_arm_code` contient un code commune ou, pour ces trois villes, un code d'arrondissement ; une jointure sur `COM` fonctionne pour toutes les stations qui ont un code. Pour agréger au niveau de la commune, il faut passer par `COMPARENT` des lignes `ARM`. Écart de comptage : 9 938 stations avec code le 2026-11-08 (M9) contre 9 939 le 2026-11-05 (M3), et 9 949 stations dans l'export contre 9 950. Explication probable : le quotidien est un état courant à J-1 (un prix par station et carburant, voir la fiche carburants), donc sa liste de stations change d'un jour à l'autre ; **non vérifié** (aucune comparaison station par station).
- La concordance avec une jointure spatiale (contours des communes) reste **non mesurée** (J6).
- Filosofi 2023 donne Paris, Lyon et Marseille **aux deux niveaux**, commune (75056, 69123, 13055) et arrondissement municipal (20, 9 et 16 lignes) (**mesuré**, `make mesure-referentiels`, voir la fiche Filosofi) : les stations codées par arrondissement trouvent une ligne Filosofi.

## 9. Décision d'ingestion

- Cadence : à la demande, une fois par millésime (**décision**).
- Tampon : base Postgres `collecte`, tables `ref_cog_commune` (clé `millesime`, `typecom`, `com`), `ref_cog_departement` (clé `millesime`, `dep`), `ref_cog_region` (clé `millesime`, `reg`), avec `charge_ts`. Les lignes d'un millésime sont remplacées en une seule transaction : le rejeu ne change rien (**mesuré**, deux chargements de suite identiques).
- Contrôles Great Expectations avant chargement (`quality/gx/ref_cog_commune.py`) : colonnes exactes ; entre 30 000 et 45 000 lignes (marge autour de la mesure) ; `TYPECOM`, `COM`, `LIBELLE` non nuls ; `TYPECOM` dans les quatre valeurs ; `COM` de forme 5 chiffres ou `2A`/`2B` plus 3 chiffres ; (`TYPECOM`, `COM`) unique ; `COMPARENT` non nul hors `COM` (documenté) ; `REG` et `DEP` non nuls pour `COM` (mesuré).
- Tests (**mesuré**, `pytest --collect-only`) : 11 dans `quality/gx/test_ref_cog_commune.py` (suite GX), 9 dans `ingestion/tests/test_referentiels.py` (lecture, BOM, colonnes inattendues, idempotence, deux millésimes, validation en échec, annulation en cours de chargement, téléchargement sans réseau), et 2 dans `ingestion/tests/test_mesures_carburants.py` pour le classement des codes de la mesure de couverture. Depuis l'ajout de Filosofi, `ingestion/tests/test_referentiels.py` compte 17 tests (9 pour le COG, 8 pour Filosofi).
- Hors périmètre : mouvements et historique des communes, contours géographiques (J6), autres millésimes.
