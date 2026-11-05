# ADR 002 : API utilisée pour accéder au catalogue Nessie

- **Statut** : remplacé par l'[ADR 004](004-catalogue-rest-ingestion.md)
- **Date** : novembre 2026

## Contexte

Nessie est déployé comme catalogue des tables Iceberg (étape 1.3). Trino doit maintenant s'y connecter. Nessie expose deux API pour cela :

- son **API native** (`/api/v2`), utilisée par le client `NessieCatalog` d'Iceberg ;
- un **catalogue REST Iceberg** (`/iceberg`), qui suit la spécification REST commune à tous les catalogues Iceberg.

Le connecteur Iceberg de Trino gère les deux : `iceberg.catalog.type=nessie` ou `iceberg.catalog.type=rest`.

Les deux API donnent accès aux mêmes tables, avec les mêmes branches : le choix porte sur la façon de se connecter, pas sur les données.

## Critères

1. **Risque d'intégration** : une première table doit être lisible depuis Trino à la fin de l'étape 1, avec SeaweedFS comme stockage.
2. **Gestion des identifiants** : le moins possible de composants qui détiennent des clés S3 avant l'étape 4.4.
3. **Branches** : pouvoir cibler une branche Nessie depuis Trino, pour le Write-Audit-Publish de l'étape 4.6.
4. **Ouverture** : pouvoir brancher d'autres clients (Spark, PyIceberg) plus tard.
5. **Réversibilité** : pouvoir changer d'avis sans migrer de données.

## Options étudiées

| Option | Avantages | Limites |
| --- | --- | --- |
| API native Nessie | Documentée côté Trino et côté Nessie. Nessie n'a besoin d'aucun accès au stockage : seul Trino détient des clés S3. Aucune configuration supplémentaire dans Nessie | Pas de gestion des vues dans Trino. PyIceberg ne propose pas de catalogue de type Nessie. Client propre à Nessie, moins standard |
| Catalogue REST Iceberg servi par Nessie | Standard partagé par tous les moteurs. Vues gérées dans Trino. Clés S3 gardées par Nessie | Nessie doit être configuré avec le warehouse et des clés S3. La documentation de Nessie 0.109.0, la version déployée, indique que Trino ne gère pas la signature des requêtes et que la délivrance d'identifiants temporaires (STS) est alors obligatoire. Je n'ai pas trouvé de documentation officielle du support de STS par SeaweedFS. Intégration validée par Nessie avec S3 et MinIO, pas avec SeaweedFS |

Dans les deux cas, une branche se choisit dans la configuration du catalogue Trino (`iceberg.nessie-catalog.ref` ou le préfixe REST) : le critère 3 ne départage pas les options.

## Décision

Trino accède à Nessie par son **API native** (`iceberg.catalog.type=nessie`), avec ses propres clés S3 pour lire et écrire dans SeaweedFS.

C'est l'option qui fait porter le moins de risque sur l'étape 1 : elle repose sur des intégrations documentées et n'ajoute pas de dépendance à STS sur SeaweedFS.

Ce choix se fait au détriment de l'ouverture (critère 4) : d'autres clients comme PyIceberg devront attendre le passage au catalogue REST.

Je considère cette décision comme réversible. Je la réexaminerai si l'un de ces cas se présente :

- l'ingestion (étape 2.3) écrit directement en Python avec PyIceberg, qui a besoin du catalogue REST ;
- le projet a besoin de vues Iceberg créées depuis Trino ;
- le support de STS par SeaweedFS est confirmé et testé, ce qui lèverait la principale limite du catalogue REST.

## Conséquences

- Nessie reste déployé sans configuration de catalogue REST (`catalog.enabled: false`) et sans accès au stockage.
- Les clés S3 de développement sont portées par Trino. Elles passeront dans un Secret Kubernetes à l'étape 4.4.
- Passer au catalogue REST plus tard demande d'activer le catalogue dans Nessie et de modifier la configuration du catalogue Trino. Les tables et l'historique Nessie restent les mêmes.
- Créer ou fusionner des branches se fera avec l'API ou le client de Nessie : Trino ne propose pas de SQL pour gérer les branches Nessie.

## Sources

- Documentation Trino, catalogues du connecteur Iceberg : https://trino.io/docs/current/object-storage/metastores.html
- Nessie, catalogue REST Iceberg : https://projectnessie.org/guides/iceberg-rest/
- Nessie, intégration avec Trino : https://projectnessie.org/iceberg/trino/
- Nessie 0.109.0, Trino avec le catalogue REST (identifiants temporaires obligatoires) : https://projectnessie.org/nessie-0-109-0/trino
