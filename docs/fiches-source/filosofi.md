# Fiche-source : Filosofi 2023, revenus et pauvreté au niveau local (INSEE)

Brouillon du 2026-11-08. Chaque résultat porte une étiquette : **mesuré** (commande de rejeu donnée), **documenté** (lu sur une page de l'INSEE ou dans le dictionnaire du fichier) ou **non vérifié**.
Rejeu : `make charger-filosofi` (télécharge, valide avec Great Expectations, charge), `make mesure-referentiels` (couverture dans le COG, valeurs vides). Code : `ingestion/referentiels.py`, `ingestion/mesures_referentiels.py`, `quality/gx/ref_filosofi_commune.py`.

## 1. Identité

- Nom : « Indicateurs sur les revenus et la pauvreté au niveau local en 2023 », dispositif Filosofi 2 (**documenté**). Éditeur : INSEE.
- Page : `https://www.insee.fr/fr/statistiques/8984752`, paru le 2026-08-06 (**documenté**). Millésime 2023, présenté comme le dernier disponible (**documenté**).
- Fichier utilisé : `FILOSOFI_CC_csv.zip`, `https://www.insee.fr/fr/statistiques/fichier/8984752/FILOSOFI_CC_csv.zip`, libellé « Revenus et pauvreté des ménages en 2023 - Tous les niveaux géographiques » (**documenté**).
- Référentiel géographique : celui en vigueur au 1er janvier 2026, donc le même que le COG 2026 chargé (**documenté**). Champ : France métropolitaine et La Réunion (**documenté**).
- Date de consultation : 2026-11-08.

## 2. Licence

- Aucune mention de licence sur la page Filosofi (**documenté**, lecture du 2026-11-08 par un sous-agent). La seule précision est le secret statistique.
- La page « Mentions légales et crédits » de l'INSEE (`https://www.insee.fr/fr/information/2008466`, mise à jour le 2026-04-13) indique : « Sauf mention contraire, les informations publiques diffusées sur ce site […] sont mises à disposition sous la Licence Ouverte / Open Licence version 2.0 (Etalab). » (**documenté**). Aucune mention contraire pour Filosofi n'a été trouvée.
- Conditions : réutilisation libre, y compris commerciale, en mentionnant « Source : Insee », la date de dernière mise à jour des données lorsqu'elle est connue, et sans altérer le sens des informations (**documenté**).

## 3. Fraîcheur

- Un millésime par an, publié avec un décalage d'environ deux ans et huit mois (millésime 2023 paru le 2026-08-06, **documenté** ; calcul sur les dates). Fréquence de publication suivante : **non vérifiée**.
- Une correction technique du 2026-08-06 est mentionnée sur la page : le taux de pauvreté n'avait pas été diffusé pour un nombre important de territoires (**documenté**). Le vide mesuré du taux (section 7, piège 2) s'explique par les statuts de confidentialité du fichier. Le lien entre cette correction et ce vide n'est pas établi (**non vérifié**), et que le fichier téléchargé l'intègre l'est aussi.
- Les requêtes `HEAD` sur le zip répondent 200 avec `Content-Type: application/zip`, **sans `Content-Length` ni `Last-Modified`** (**mesuré**, 2026-11-08).

## 4. Schéma

Le zip contient deux fichiers (**mesuré**, 2026-11-08) :
- `DS_FILOSOFI_CC_2023_data.csv` : 57 830 279 octets décompressés (4 969 307 comprimés), 1 123 173 lignes de données.
- `DS_FILOSOFI_CC_2023_metadata.csv` : 2 104 942 octets décompressés, dictionnaire des modalités (colonnes `COD_VAR`, `LIB_VAR`, `COD_MOD`, `LIB_MOD`, `GEO_OBJECT`).
- Zip : 5 362 858 octets, sha256 `f3822a78e3f55fa8…` (identique à deux téléchargements, **mesuré**).

Format : **long** (une ligne par zone et par mesure), séparateur point-virgule, champs de texte entre guillemets (sauf `TIME_PERIOD`), fins de ligne LF, UTF-8 sans BOM (**mesuré**).

| Colonne | Contenu | Mesuré |
|---|---|---|
| `GEO` | code de la zone | 5 caractères pour tous les `GEO_OBJECT` = `COM` |
| `GEO_OBJECT` | niveau géographique | 12 modalités : `COM` 938 736 lignes, `UU2020` 65 610, `BV2022` 45 630, `EPCI` 33 345, `AAV2020` 18 603, `ARR` 8 748, `ZE2020` 7 857, `DEP` 2 619, `ARM` 1 215, `OTHER` 405, `REG` 378, `FRANCE` 27 |
| `FILOSOFI_MEASURE` | mesure | 27 mesures, chacune sur 41 599 zones (27 × 41 599 = 1 123 173) |
| `UNIT_MEASURE` | unité | `EUR_YR` euros par an, `NR` nombre, `PT` pourcentage (dictionnaire) |
| `UNIT_MULT` | multiplicateur | `0` (unités) |
| `CONF_STATUS` | confidentialité | `F` libre : 1 089 662 lignes ; `C` information statistique confidentielle : 33 511 lignes |
| `OBS_STATUS` | statut de l'observation | `A` normale : 85 968 lignes ; `O` valeur manquante : 1 037 205 lignes |
| `TIME_PERIOD` | période | 2023 pour toutes les lignes |
| `OBS_VALUE` | valeur | vide pour toute ligne `O` et pour toute ligne `A` de confidentialité `C` ; renseignée pour les 52 457 lignes `A` libres |

Mesures utilisées : `MED_SL` (niveau de vie médian, euros par an) et `PR_MD60` (taux de pauvreté au seuil de 60 % de la médiane, pourcentage) (**documenté**, dictionnaire). Format des valeurs non vides (**mesuré**) : `MED_SL` toujours entier ; `PR_MD60` entier (4 859 valeurs) ou décimal avec point (428 valeurs, exemple `13.4`).
Les autres mesures (déciles, Gini, parts des revenus) existent dans le fichier mais ne sont pas chargées ; la page indique qu'elles ne sont publiées qu'aux niveaux région, département et France métropolitaine (**documenté**).

## 5. Volumétrie

- Niveau `COM` : 34 768 communes × 27 mesures = 938 736 lignes ; niveau `ARM` : 45 arrondissements × 27 = 1 215 lignes (**mesuré**).
- Table `ref_filosofi_commune` après filtre (niveaux `COM` et `ARM`, mesures `MED_SL` et `PR_MD60`) : 34 813 lignes (**mesuré**, 34 768 + 45).
- Évolution : un millésime par an (**non vérifié**).

## 6. Historique

- Le fichier ne contient que le millésime 2023. Les millésimes 2012 à 2021 relèvent de l'ancien dispositif Filosofi et ne sont **pas directement comparables** (**documenté**, mot pour mot sur la page).
- Autres millésimes : non téléchargés.

## 7. Pièges

1. **Format long** : une ligne par (zone, mesure) ; il faut pivoter pour avoir une ligne par commune. Le chargeur garde seulement `MED_SL` et `PR_MD60` des niveaux `COM` et `ARM`.
2. **Le taux de pauvreté est vide pour 84,9 % des communes** : `PR_MD60` vide pour 29 526 communes sur 34 768, `MED_SL` vide pour 3 974 (11,4 %) (**mesuré**). Autrement dit, le taux de pauvreté est disponible pour 5 242 communes (15,1 %) et le niveau de vie médian pour 30 794 (88,6 %) (**mesuré**). **Ce vide s'explique par les statuts mesurés** : sur les 29 526 taux vides, 29 512 sont en confidentialité `C` (statut `A`, valeur masquée) et 14 en statut `O` (valeur manquante) (**mesuré**). Détail des statuts des communes (`MED_SL` statut/confidentialité, `PR_MD60` statut/confidentialité) : A/F et A/C pour 25 552 (niveau de vie renseigné, taux confidentiel), A/F et A/F pour 5 242 (les deux renseignés), A/C et A/C pour 3 960 (les deux confidentiels), O/F et O/F pour 14 (les deux manquants). La règle de confidentialité qui masque le taux pour autant de communes (secret statistique, **documenté** en termes généraux) n'est pas détaillée dans les fichiers lus : seuil exact **non vérifié**. Le lien avec la correction du 2026-08-06 citée par la page n'est pas établi (**non vérifié**).
   **Conséquence de conception** : `MED_SL` est l'indicateur principal (disponible pour 88,6 % des communes), `PR_MD60` un complément (15,1 %). Décision à confirmer en J6 (jointure aux stations) et J7 (modèles).
3. **14 communes ont les deux valeurs manquantes (statut `O`)**, dont les 7 communes que la page cite comme sans données (09304 Suzan, 15031 Celles, 15035 Chalinargues, 15047 Chavagnac, 15171 Sainte-Anastasie, 85165 L'Oie, 85212 Sainte-Florence). Les 7 autres, **absentes de la liste de la page** : 54310 Leménil-Mitry (Meurthe-et-Moselle) et six communes de la Meuse, 55039 Beaumont-en-Verdunois, 55050 Bezonvaux, 55139 Cumières-le-Mort-Homme, 55189 Fleury-devant-Douaumont, 55239 Haumont-près-Samogneux, 55307 Louvemont-Côte-du-Poivre (**mesuré**). Pourquoi ces 7 autres sont en statut `O` : **non vérifié**. Hypothèse, non vérifiée : communes sans habitants. Les 7 annoncées ont une ligne dans le fichier avec une valeur vide, elles ne sont pas absentes.
4. **107 communes du COG n'ont aucune ligne** : 105 en outre-mer (971 : 32, 972 : 34, 973 : 22, 976 : 17), hors champ (Guadeloupe, Martinique, Guyane, Mayotte, **documenté** : « France métropolitaine et La Réunion »), et 2 dans le Finistère, 29083 Île-de-Sein et 29084 Île-Molène, dont l'absence n'est **pas expliquée** (**mesuré**).
5. **Paris, Lyon et Marseille figurent aux deux niveaux** : les communes 75056, 69123 et 13055 (niveau `COM`, valeurs renseignées) et leurs arrondissements municipaux (niveau `ARM` : 20, 9 et 16 codes, valeurs toutes renseignées) (**mesuré**). Aucun code `ARM` n'apparaît aussi au niveau `COM`.
6. **Codes `GEO` des autres niveaux** : la forme des codes des niveaux autres que `COM` et `ARM` (par exemple `AAV2020`) n'est **pas mesurée** : ne pas les mélanger avec des codes commune. Le code à 5 caractères est **mesuré** pour `COM` seulement.
7. **Comparabilité** : pas de comparaison directe avec les millésimes 2012 à 2021 (**documenté**).

## 8. Requêtes et commandes d'échantillon

```text
make charger-filosofi          télécharge, valide (GX), remplace le millésime 2023 : 34 813 lignes
make mesure-referentiels       couverture dans le COG, valeurs vides, les 14 communes manquantes, Paris/Lyon/Marseille
make mesure-carburants M=m10   part des stations du quotidien avec MED_SL et PR_MD60 renseignés
```

```sql
SELECT geo_object, count(*), count(*) FILTER (WHERE med_sl IS NULL) AS med_sl_vide, count(*) FILTER (WHERE pr_md60 IS NULL) AS pr_md60_vide
FROM ref_filosofi_commune WHERE millesime = 2023 GROUP BY 1;
-- ARM 45 / 0 / 0 ; COM 34768 / 3974 / 29526
```

Couverture dans `ref_cog_commune` (millésime 2026) par `TYPECOM` (**mesuré**, `make mesure-referentiels`, 2026-11-08) :
- Niveau `COM` : 34 768 codes, tous de type `COM` dans le COG (0 absent du COG, 0 type `COMD` ou `COMA`).
- Niveau `ARM` : 45 codes, tous de type `ARM` dans le COG (0 absent).
- Communes `COM` du COG sans ligne Filosofi : 107 (voir piège 4). Codes Filosofi `COM` qui ne sont pas `COM` dans le COG : 0.
- Jointure avec le flux quotidien : une station de Paris, Lyon ou Marseille porte un code d'arrondissement (fiche COG), qui existe au niveau `ARM` de Filosofi avec des valeurs renseignées ; une station d'une autre commune porte un code `COM`. Part des stations du quotidien avec Filosofi renseigné (**mesuré**, `make mesure-carburants M=m10`, 2026-11-08, via `com_arm_code`, arrondissements compris) : sur 9 949 stations, 11 sans code et 0 code sans ligne Filosofi ; `MED_SL` renseigné pour 9 926 (99,8 %), `PR_MD60` renseigné pour 7 683 (77,2 %), les deux pour 7 683 (77,2 %). Sur les 9 938 stations avec code : 99,9 % et 77,3 %. Les stations sont dans des communes plus peuplées que la moyenne des communes (15,1 % des communes ont un taux, 77,2 % des stations) : cette lecture est une hypothèse, **non vérifiée** (aucune mesure de population).

## 9. Décision d'ingestion

- Cadence : à la demande, une fois par millésime (**décision**).
- Tampon : base Postgres `collecte`, table `ref_filosofi_commune` (clé `millesime`, `geo_object`, `codgeo`), colonnes `med_sl`, `med_sl_statut`, `med_sl_conf`, `pr_md60`, `pr_md60_statut`, `pr_md60_conf`, `charge_ts`. Les valeurs vides restent `NULL`, les statuts sont conservés pour distinguer « manquant » de « confidentiel ». Remplacement du millésime en une transaction : le rejeu ne change rien (**mesuré**, deux chargements de suite à 34 813 lignes, même empreinte).
- Lecture du fichier de 58 Mo en flux, directement dans le zip, sans extraction ni chargement complet en mémoire (**mesuré** : mémoire du conteneur airflow 2,257 Gio avant et 2,265 Gio après).
- Contrôles Great Expectations avant chargement (`quality/gx/ref_filosofi_commune.py`) : colonnes exactes ; entre 30 000 et 40 000 lignes ; niveau dans `COM` ou `ARM` ; code de 5 caractères (ou `2A`/`2B` et 3 chiffres) ; (`GEO_OBJECT`, `CODGEO`) unique ; statuts dans `A`/`O` et `F`/`C` ; `MED_SL` strictement positif ; `PR_MD60` entre 0 et 100 ; valeur vide si statut `O` ; valeur renseignée si statut `A` et libre ; au moins 80 % de `MED_SL` renseignés et 5 % de `PR_MD60` renseignés (marges autour des mesures de 88,6 % et 15,1 %, pas des valeurs documentées).
- Tests (**mesuré**, `pytest --collect-only`) : 12 dans `quality/gx/test_ref_filosofi_commune.py`, 8 Filosofi dans `ingestion/tests/test_referentiels.py` (pivot, en-tête, période, valeur non numérique, zip sans données, idempotence et millésimes, validation en échec, téléchargement sans réseau), 4 dans `ingestion/tests/test_mesures_referentiels.py`.
- Hors périmètre : les autres mesures et niveaux géographiques du fichier, les autres millésimes, la jointure aux stations dans le modèle (la couverture est mesurée, section 8).
