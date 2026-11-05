# ADR 004 : catalogue REST Iceberg pour l'ingestion

- **Statut** : accepté
- **Date** : novembre 2026
- **Remplace** : [ADR 002](002-api-catalogue-nessie.md)

## Contexte

L'ADR 002 a branché Trino sur l'API native de Nessie, et réservé le catalogue REST Iceberg à plus tard. Il citait comme premier cas de réexamen une ingestion qui écrirait directement en Python avec PyIceberg. C'est le cas maintenant.

Le DAG de chargement de l'historique doit écrire chaque mois dans une table Iceberg de la couche bronze, en remplaçant les lignes du mois, en un seul commit. J'ai écarté deux façons de passer par Trino :

- la procédure `add_files`, qui référence les fichiers sans les réécrire. Le Parquet exporté par ODRÉ stocke ses horodatages en millisecondes, alors qu'Iceberg les attend en microsecondes, et ses colonnes ne portent pas d'identifiant de champ ;
- un `MERGE` dont les données sont envoyées en SQL. Il reste atomique, mais il fait environ 400 Ko de texte par mois, il est lent, et il ne supprime pas une ligne disparue de la source.

PyIceberg écrit le Parquet lui-même et remplace un mois avec `overwrite`, en un commit. Mais il ne propose pas de catalogue de type Nessie : il lui faut le catalogue REST.

Deux questions restent ouvertes :

1. Trino doit-il quitter l'API native pour le catalogue REST ?
2. Comment PyIceberg accède-t-il aux fichiers dans SeaweedFS ?

Pour la seconde, le catalogue REST de Nessie 0.109.0 propose trois mécanismes :

- les **identifiants temporaires** (« vended credentials ») : Nessie obtient des identifiants auprès d'un service STS et les transmet au client ;
- la **signature des requêtes** : le client fait signer chaque requête S3 par Nessie, qui seul détient les clés ;
- les **clés du client** : le client utilise ses propres clés, et Nessie ne lui transmet aucun accès.

Dans les trois cas, Nessie écrit lui-même les métadonnées des tables : il a besoin de ses propres clés S3.

## Critères

Ce sont les critères de l'ADR 002, appliqués à l'ingestion :

1. **Risque d'intégration** : le DAG doit pouvoir charger l'historique avec SeaweedFS comme stockage.
2. **Gestion des identifiants** : le moins possible de composants qui détiennent des clés S3 avant l'étape 4.4.
3. **Ouverture** : un client Python standard pour écrire dans le lakehouse.
4. **Réversibilité** : pouvoir changer d'avis sans migrer de données.

## Options étudiées

### Accès de Trino au catalogue

| Option | Avantages | Limites |
| --- | --- | --- |
| Trino reste sur l'API native | Déjà en place et testé par le smoke test. Aucun besoin de STS | Deux API de catalogue à faire cohabiter |
| Trino passe au catalogue REST | Une seule API pour tous les clients. Vues gérées dans Trino | La documentation de Nessie 0.109.0 indique que Trino ne gère pas la signature des requêtes : il lui faut des identifiants temporaires, donc STS. Le risque relevé dans l'ADR 002 est inchangé : je n'ai pas trouvé de documentation officielle du support de STS par SeaweedFS, et je ne l'ai pas testé |

### Accès de PyIceberg à SeaweedFS

| Option | Avantages | Limites |
| --- | --- | --- |
| Identifiants temporaires (STS) | Aucune clé permanente chez le client. Droits limités à chaque table | Même risque STS que pour Trino |
| Signature des requêtes par Nessie | Seul Nessie détient des clés. Aucun besoin de STS | PyIceberg ne gère la signature distante qu'avec `FsspecFileIO` : une dépendance en plus (`s3fs`), pas l'implémentation par défaut. Combinaison PyIceberg, signature Nessie et SeaweedFS non documentée |
| Clés S3 propres à PyIceberg | Mécanisme le plus simple, géré par `PyArrowFileIO`, l'implémentation par défaut. Ne dépend que de l'API S3 de SeaweedFS, déjà utilisée par Trino | Airflow détient des clés S3 : un composant de plus qui en possède |

## Décision

- **L'ingestion écrit avec PyIceberg via le catalogue REST Iceberg de Nessie** (`/iceberg`). Le catalogue est activé dans Nessie avec un warehouse `warehouse` pointant vers `s3://warehouse`.
- **Trino reste sur l'API native.** Passer au catalogue REST le ferait dépendre de STS. Les deux API donnent accès aux mêmes tables et aux mêmes branches : une table écrite par PyIceberg est lue par Trino sans migration.
- **PyIceberg accède à SeaweedFS avec ses propres clés S3**, par `PyArrowFileIO`, sans identifiants temporaires ni signature des requêtes. La signature est désactivée dans Nessie, pour qu'il ne propose pas aux clients un mécanisme qu'ils n'utilisent pas.
- **Nessie reçoit ses propres clés S3** (mode `STATIC`) pour écrire les métadonnées des tables. Le chart Nessie les lit dans un Secret Kubernetes.

Les clés restent des valeurs de développement jusqu'à l'étape 4.4.

## Conséquences

- Trois composants détiennent désormais des clés S3 (Trino, Nessie et Airflow), contre un seul avec l'ADR 002 : le critère 2 recule. À l'étape 4.4, chacun recevra sa propre identité SeaweedFS, avec des droits limités à ce dont il a besoin.
- Le Secret du chart Nessie est le premier Secret Kubernetes du projet. Il contient des valeurs de développement, comme les fichiers de valeurs.
- Le catalogue REST et l'API native doivent cohabiter. La sous-étape 2.7 le vérifie : une table écrite par PyIceberg doit être lisible depuis Trino, et son commit visible dans Nessie. Si ce n'est pas le cas, cette décision sera réexaminée avant d'écrire le DAG.
- L'image Airflow doit contenir PyIceberg. Elle devient une image personnalisée (sous-étape 2.8).

Je réexaminerai cette décision dans deux cas :

- la signature des requêtes est testée avec PyIceberg et SeaweedFS : elle permettrait de retirer les clés S3 d'Airflow ;
- le support de STS par SeaweedFS est confirmé et testé : il permettrait de passer Trino au catalogue REST, avec des identifiants temporaires pour tous les clients.

## Sources

- ADR 002, risque STS et documentation Trino du catalogue REST de Nessie 0.109.0 : [002-api-catalogue-nessie.md](002-api-catalogue-nessie.md)
- Nessie, catalogue REST Iceberg : https://projectnessie.org/guides/iceberg-rest/
- Chart Helm Nessie 0.109.0, options `catalog.storage.s3` (`authType`, `requestSigningEnabled`, `clientIam`, `accessKeySecret`) : https://github.com/projectnessie/nessie/blob/nessie-0.109.0/helm/nessie/values.yaml
- PyIceberg, configuration du FileIO (signature distante limitée à `FsspecFileIO`) : https://py.iceberg.apache.org/configuration/
- Trino 484, procédure `add_files` du connecteur Iceberg : https://trino.io/docs/484/connector/iceberg.html
