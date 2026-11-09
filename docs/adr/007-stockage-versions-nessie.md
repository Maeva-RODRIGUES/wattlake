# ADR 007 : stockage des versions de Nessie

- **Statut** : accepté
- **Date** : novembre 2026

## Contexte

Nessie enregistre l'état du catalogue dans un « version store » : les références (branches, tags), les commits et, pour chaque table, l'emplacement de ses métadonnées Iceberg. Les fichiers des tables, eux, sont dans SeaweedFS.

Depuis l'étape 1.3, ce stockage est en mémoire (`IN_MEMORY`). Chaque redémarrage du pod Nessie efface le catalogue : les fichiers restent dans SeaweedFS, mais plus aucune table n'est connue. C'est arrivé deux fois le même jour, après un redémarrage du cluster et après le passage aux Secrets (ADR 006). Chaque fois, il a fallu recharger les 178 mois de la couche bronze, environ 18 minutes, puis reconstruire silver et gold.

L'étape 4.6 (Write-Audit-Publish) reposera sur les branches Nessie : elles doivent survivre à un redémarrage.

## Critères

1. **Persistance** : le catalogue survit au redémarrage du pod Nessie et du cluster (`make cluster-stop` puis `make cluster-start`).
2. **Support par Nessie** : un stockage recommandé pour la production par la documentation de Nessie 0.109.0.
3. **Peu de pièces en plus** à déployer et à exploiter en local.
4. **Isolation** : Nessie ne doit pas pouvoir lire ni modifier les données d'Airflow, et inversement.

## Options étudiées

| Option | Avantages | Limites |
| --- | --- | --- |
| JDBC2 sur l'instance PostgreSQL existante (rôle et base `nessie`) | Aucune instance de plus : PostgreSQL est déjà déployé pour Airflow (ADR 003). PostgreSQL est le seul SGBD que la documentation de Nessie classe « production » pour JDBC2. Isolation par rôle et par base | Nessie dépend de PostgreSQL : si la base est arrêtée, le catalogue l'est aussi, et Airflow avec. Une panne de l'instance touche les deux |
| ROCKSDB sur un volume persistant | Aucune dépendance externe, le plus simple. Classé « production » par Nessie | Classé « single node only » : le volume ne s'attache qu'à un pod, une seule réplique de Nessie possible. Moins représentatif d'un déploiement réel |
| JDBC2 sur une instance PostgreSQL dédiée | Pannes et ressources indépendantes d'Airflow | Une deuxième base à déployer, sauvegarder et mettre à jour, sans bénéfice en local |
| MongoDB, Cassandra, DynamoDB, Bigtable | Supportés par Nessie | Un nouveau système à opérer, sans rapport avec le reste de la stack |

## Décision

**Nessie utilise le version store `JDBC2`, sur l'instance PostgreSQL de [infra/postgres](../../infra/postgres/)**, dans une base `nessie` qui appartient à un rôle `nessie`.

- **Création de la base** : le script d'initialisation de PostgreSQL (`02-nessie.sh`) crée le rôle et la base. Nessie crée lui-même ses tables (`refs2` et `objs2`) au premier démarrage.
- **Isolation** : chaque base n'accepte que les connexions de son propriétaire (`REVOKE CONNECT ... FROM PUBLIC`). Le rôle `nessie` ne peut pas se connecter à la base `airflow`, ni le rôle `airflow` à la base `nessie`.
- **Identifiants** : le mot de passe du rôle `nessie` est dans le Secret `nessie-db-credentials`, créé par `make secrets` (ADR 006). Il est lu par PostgreSQL à l'initialisation, et par Nessie.

## Conséquences

- Un redémarrage de Nessie ne perd plus rien. Le rechargement complet de bronze ne sert plus qu'après une perte réelle de la base `nessie`.
- PostgreSQL devient un point de dépendance commun à Airflow et à Nessie. En production, je séparerais les instances, ou j'utiliserais une base gérée avec haute disponibilité et sauvegardes.
- Les sauvegardes de la base `nessie` ne sont pas en place : en local, une perte du volume PostgreSQL se rattrape par un rechargement. Elles seraient indispensables en production, car le catalogue y devient la référence de l'état des tables.
- Le passage d'`IN_MEMORY` à `JDBC2` perd une dernière fois le catalogue : un contenu en mémoire ne peut pas être exporté, l'outil d'export de Nessie (Nessie Server Admin Tool) lisant directement le version store.
- La documentation de Nessie note que les bases relationnelles peuvent devenir un goulot d'étranglement quand beaucoup de commits concurrents visent la même branche. Ce n'est pas le cas ici : le DAG bronze n'écrit qu'un mois à la fois.
- Je réexaminerai cette décision si Nessie doit passer à plusieurs réplicas sous forte charge d'écriture, ou si la base PostgreSQL doit être séparée.

## Sources

- Nessie 0.109.0, configuration du version store : tableau des bases supportées (« RocksDB : production, single node only », « PostgreSQL : production » pour JDBC2, MariaDB et MySQL « experimental ») et réglages JDBC. https://projectnessie.org/nessie-0-109-0/configuration/
- Chart Nessie 0.109.0, valeurs `versionStoreType` et `jdbc` (`jdbcUrl`, `secret`).
- PostgreSQL 17, privilèges par défaut : « the default privileges granted to PUBLIC are as follows: CONNECT and TEMPORARY (create temporary tables) privileges for databases ». https://www.postgresql.org/docs/17/ddl-priv.html
