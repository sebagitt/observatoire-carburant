---
name: debrief-jour
description: "Produit le debrief de fin de journée du projet (5 blocs), l'enregistre dans docs/journal/ et régénère HANDOFF.md. À utiliser quand l'utilisateur dit debrief, fin de journée, clôture du jour, ou demande de mettre à jour HANDOFF.md."
---

# Debrief de fin de journée

L'utilisateur est en apprentissage : chaque notion nouvelle est expliquée simplement, chaque affirmation est vérifiée.

## Avant d'écrire

1. Relis le critère « terminé » du jour (spec ou HANDOFF.md).
2. Lance `make test` et note le résultat réel. Ne déclare rien de vert sans l'avoir exécuté.
3. Relève ce qui s'est vraiment passé : `git log` du jour, fichiers modifiés, erreurs rencontrées.
4. N'invente aucun chiffre. Si une mesure manque, écris « non mesuré ».

## Le debrief : docs/journal/J<n>.md, en 5 blocs

1. **Fait** : 3 à 5 puces, avec les chiffres vérifiés.
2. **Concepts du jour** : pour chaque notion nouvelle, une définition en une phrase et une analogie courte.
3. **Décisions et pourquoi** : le choix, l'alternative écartée, la raison.
4. **Pièges rencontrés** : symptôme, cause, correction.
5. **Pour reproduire** : commandes exactes (PowerShell sous Windows) et résultat attendu.

Ajoute si utile une section « À surveiller » et les nouveaux termes du glossaire.

## HANDOFF.md (racine du repo)

Régénère le fichier en entier, avec ces sections et dans cet ordre :

1. Tâche suivante (jour, contenu, critère de fin)
2. État (ce qui tourne, mesures réelles, ce qui manque)
3. Décisions d'architecture
4. Schémas (tables et colonnes)
5. Commandes qui marchent
6. Erreurs résolues
7. Points ouverts

## Règles

- Français, jamais de tirets cadratin.
- docs/journal/ et docs/guide/ sont ignorés par git ; HANDOFF.md est commité.
- Le Guide (document Claude) se met à jour depuis le chat, pas depuis ce dépôt : signale à l'utilisateur ce qu'il faut y reporter.
- Ne commite pas sans demande. Aucune mention Co-Authored-By.
