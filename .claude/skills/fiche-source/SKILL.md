---
name: fiche-source
description: "Rédige la fiche de reconnaissance d'une source de données (schéma, volumétrie, fraîcheur, licence, pièges, requête d'échantillon) dans docs/fiches-source/. À utiliser avant d'ingérer une nouvelle source ou quand l'utilisateur demande une fiche-source."
---

# Fiche-source

Objectif : savoir exactement ce que contient une source avant d'écrire la moindre ligne d'ingestion.

## Méthode

1. Délègue la reconnaissance au subagent `explorateur-sources` (il renvoie un résumé court sans remplir le contexte).
2. Mesure toi-même ce qui compte, avec une commande rejouable (script ou requête). Ne recopie pas un chiffre de documentation comme s'il était mesuré.
3. Rédige la fiche dans `docs/fiches-source/<source>.md`.

## Contenu de la fiche

1. **Identité** : nom, éditeur, URL officielle, date de consultation.
2. **Licence** et conditions de réutilisation.
3. **Fraîcheur** : fréquence de mise à jour côté source, décalage constaté.
4. **Schéma** : tableau colonne, type, exemple, clé, taux de nulls mesuré.
5. **Volumétrie** : nombre de lignes, taille, évolution.
6. **Historique** : disponible ou non (backfill possible ou pas).
7. **Pièges** : fuseaux horaires, formats, doublons, valeurs aberrantes, champs ambigus.
8. **Requête ou commande d'échantillon** : reproductible, avec le résultat attendu.
9. **Décision d'ingestion** : cadence, clé de dédoublonnage, tampon, tests prévus.

## Règles

- Sépare toujours « mesuré » de « lu dans la documentation » et de « non vérifié ».
- La fiche est commitée (docs/fiches-source/ n'est pas ignoré).
- Français, jamais de tirets cadratin.
