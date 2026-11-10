# ADR 008 : Write-Audit-Publish avec les branches Nessie

- **Statut** : accepté
- **Date** : novembre 2026

## Contexte

Le DAG `dbt_transformations` lance un `dbt build` qui reconstruit silver puis gold, puis exécute les tests de qualité. Jusqu'ici, il écrivait directement sur la branche `main` de Nessie, celle que lisent les consommateurs. Chaque modèle remplace sa table par un `CREATE OR REPLACE TABLE`, qui est atomique table par table. Un test qui échoue arrête bien les modèles en aval, mais la table qu'il vient de tester est déjà publiée dans `main`, et les autres tables peuvent dater d'une exécution précédente. Le contrôle qualité arrive donc trop tard.

Le Write-Audit-Publish sépare trois temps :

- **Write** : écrire là où personne ne lit ;
- **Audit** : tester ce qui a été écrit ;
- **Publish** : rendre le résultat visible d'un seul coup, seulement si les tests passent.

Nessie a été choisi comme catalogue pour ses branches (ADR 001), et sa persistance dans PostgreSQL (ADR 007) permet maintenant de les utiliser.

Deux contraintes viennent des choix précédents :

- Trino est relié à une branche fixe dans la configuration de son catalogue (`iceberg.nessie-catalog.ref`), et n'a pas de SQL pour changer de branche (ADR 002).
- Créer, supprimer ou fusionner une branche se fait avec l'API de Nessie, pas avec Trino.

## Critères

1. **Lecteurs protégés** : un consommateur de `main` ne voit jamais un build partiel ni un build dont un test a échoué.
2. **Publication atomique** : silver et gold arrivent ensemble dans `main`, en un seul commit.
3. **Diagnostic** : après un échec, les données fautives restent consultables.
4. **Simplicité** : peu de pièces en plus, dans une stack locale à un seul développeur.
5. **Pas de nouvelle dépendance** dans l'image Airflow.

## Options étudiées

### Périmètre

| Option | Avantages | Limites |
| --- | --- | --- |
| Transformation dbt seulement (silver et gold) | C'est là que vivent les tests de qualité et ce que lisent les consommateurs. Un seul DAG à modifier | Bronze reste écrit directement sur `main` |
| dbt et ingestion bronze | Toute la chaîne passe par une branche | Bronze est brut et idempotent, et son seul contrôle, le schéma, a déjà lieu avant l'écriture. Il faudrait un second catalogue PyIceberg sur la branche. Deux fois plus de travail pour un gain faible |

### Branche utilisée par Trino

| Option | Avantages | Limites |
| --- | --- | --- |
| Une branche fixe `audit`, recréée depuis `main` à chaque exécution, et un catalogue Trino `iceberg_audit` relié à elle | Configuration statique, versionnée dans `infra/trino/values.yaml`. Une seule exécution à la fois (`max_active_runs=1`) : une branche suffit | Deux exécutions parallèles se marcheraient dessus, ce qu'interdit déjà le DAG |
| Une branche par exécution (`audit-<run_id>`), avec un catalogue Trino créé dynamiquement (`CREATE CATALOG`) | Isolation complète entre exécutions | Il faut activer la gestion dynamique des catalogues (`catalog.management=dynamic`) et nettoyer catalogues et branches. Aucun gain tant qu'une seule exécution tourne à la fois |

### Après un test en échec

| Option | Avantages | Limites |
| --- | --- | --- |
| Garder la branche `audit` jusqu'à l'exécution suivante | Les tables fautives restent interrogeables dans `iceberg_audit` | Une branche reste en place entre deux exécutions |
| Supprimer la branche aussitôt | Aucun reste | Plus rien à examiner |

## Décision

**Le DAG `dbt_transformations` applique le Write-Audit-Publish sur une branche Nessie fixe, `audit`, en trois tâches :**

1. **`create_audit_branch`** supprime la branche `audit` si elle existe, puis la recrée depuis la tête de `main`. La tâche transmet le hash de départ aux suivantes.
2. **`dbt_build`** lance `dbt build --target audit`. La cible `audit` de `transform/profiles.yml` utilise le catalogue Trino `iceberg_audit`, relié à la branche `audit`. La source bronze suit aussi la cible (`database: "{{ target.database }}"`) : dbt lit bronze tel qu'il était à la création de la branche.
3. **`publish`**, lancé seulement si `dbt_build` réussit, fusionne `audit` dans `main` par l'API v2 de Nessie, en un seul commit.

Le client Nessie (`wattlake_ingestion.nessie`) appelle l'API v2 avec `httpx`, déjà dans l'image. Je n'utilise pas `pynessie`, le client Python de Nessie : son dépôt est archivé, et sa dernière version (0.67.0) date de février 2024.

**La fusion vérifie les hashes de part et d'autre :**

- Côté source, elle fusionne le hash d'`audit` lu juste après les tests (`fromHash`), et non la tête de la branche au moment de la fusion. Ce qui est publié est exactement ce qui a été testé.
- Côté cible, elle vise `main@<hash de départ>`. Nessie cherche alors les conflits avec tout ce qui a changé dans `main` depuis la création de la branche. Un chargement bronze entre-temps ne touche pas les mêmes tables et ne bloque pas la fusion. Une modification de silver ou de gold dans `main` la ferait échouer, sans rien fusionner.

**Après un test en échec, la branche `audit` est gardée** pour l'enquête, jusqu'à la prochaine exécution qui la recrée. `main` reste tel qu'il était.

Trino garde la branche par son nom, pas par son hash. J'ai vérifié dans le code de Trino 484 et d'Iceberg 1.12.0, qu'il embarque :

- à chaque lecture d'une table, la référence est relue par son nom (`IcebergNessieTableOperations.refresh`, puis `NessieIcebergClient.refresh`) ;
- pour une branche, les listes de tables sont aussi demandées par nom (`withReference`).

Une branche `audit` supprimée puis recréée est donc relue avec son nouveau hash. La référence n'est chargée qu'à la première requête (`Suppliers.memoize`) : le catalogue `iceberg_audit` ne fait pas échouer le démarrage de Trino si la branche n'existe pas encore.

## Conséquences

- Les consommateurs de `main` voient silver et gold passer d'un build testé au suivant, en un commit Nessie. Un échec de test laisse `main` inchangé et la tâche `dbt_build` en échec dans Airflow.
- Un build fautif s'examine dans Trino par le catalogue `iceberg_audit`, par exemple `SELECT ... FROM iceberg_audit.silver.eco2mix_national`, et se compare à `iceberg.silver.eco2mix_national`.
- Trino a un catalogue de plus, avec les mêmes clés S3 que le catalogue `iceberg`.
- `make dbt ARGS="build"`, lancé depuis un poste, garde la cible `dev` et écrit toujours directement sur `main`. Le Write-Audit-Publish concerne le DAG. Pour tester à la main sans toucher `main`, on passe `--target audit`.
- Si la fusion réussit mais que la réponse se perd, la nouvelle tentative de `publish` échoue sur un conflit : `main` contient déjà les tables. L'état de `main` reste correct, seule la tâche est en échec.
- Je réexaminerai cette décision si plusieurs builds doivent tourner en parallèle (une branche par exécution deviendrait nécessaire), ou si bronze reçoit ses propres tests de qualité.

## Sources

- Nessie 0.109.0, API REST v2 (`HttpTreeApi`) : création d'une référence depuis un hash, suppression avec hash attendu, fusion (« The state of contents specified by the 'branch' reference will be used for detecting conflicts »). https://github.com/projectnessie/nessie/blob/nessie-0.109.0/api/model/src/main/java/org/projectnessie/api/v2/http/HttpTreeApi.java
- Nessie 0.109.0, forme `name@hash` d'une branche cible de fusion (« The 'hash' commit must be reachable from the current HEAD of the branch »). https://github.com/projectnessie/nessie/blob/nessie-0.109.0/api/model/src/main/java/org/projectnessie/api/v2/doc/ApiDoc.java
- Trino 484, opérations de table Nessie (relecture de la référence à chaque lecture). https://github.com/trinodb/trino/blob/484/plugin/trino-iceberg/src/main/java/io/trino/plugin/iceberg/catalog/nessie/IcebergNessieTableOperations.java
- Trino 484, client Nessie unique créé depuis la configuration du catalogue. https://github.com/trinodb/trino/blob/484/plugin/trino-iceberg/src/main/java/io/trino/plugin/iceberg/catalog/nessie/IcebergNessieCatalogModule.java
- pynessie : dépôt archivé, https://github.com/projectnessie/pynessie ; version 0.67.0 du 9 février 2024, https://pypi.org/project/pynessie/
- Iceberg 1.12.0, `NessieIcebergClient` (chargement paresseux, `refresh`, `withReference`). https://github.com/apache/iceberg/blob/apache-iceberg-1.12.0/nessie/src/main/java/org/apache/iceberg/nessie/NessieIcebergClient.java
