# ADR 013 : maintenance des tables Iceberg

- **Statut** : accepté
- **Date** : novembre 2026

## Contexte

Une table Iceberg ne modifie jamais un fichier : chaque écriture ajoute des fichiers et un nouveau snapshot. Nessie garde en plus l'historique de chaque branche, commit par commit. Sans maintenance, deux problèmes grandissent :

- **Les petits fichiers.** Le writer temps réel ajoute un fichier par lot, soit environ 100 fichiers de 30 Ko par jour dans `bronze.eco2mix_national_realtime` (44 fichiers au 13 novembre 2026). Chaque lecture ouvre tous ces fichiers.
- **Les fichiers que plus rien n'utilise.** Le DAG dbt réécrit silver et gold toutes les heures, environ 14 Mo par exécution (ADR 012). Les anciennes versions restent dans SeaweedFS. Le 13 novembre 2026, SeaweedFS occupe 699 Mo pour environ 37 Mo de tables actuelles.

Ce que permet la stack, vérifié dans le code et la documentation des versions déployées :

- **Trino 484, catalogue Nessie** : `ALTER TABLE … EXECUTE optimize` compacte les fichiers d'une table. Trino ne supprime en revanche jamais de fichiers sur un catalogue Nessie : « The table folder may be referenced by other branches. Therefore, dropping the table should not delete the data. Nessie GC tool can be used to clean up the expired data » (`TrinoNessieCatalog`). Les procédures `expire_snapshots` et `remove_orphan_files` d'Iceberg ne voient qu'une branche et ne conviennent pas.
- **Nessie GC 0.109.0**, image `ghcr.io/projectnessie/nessie-gc` : marque le contenu vivant sur toutes les branches (avec une rétention, `--default-cutoff`), en déduit les fichiers Iceberg qui ne servent plus, et les supprime. `--inmemory` évite une base de données, pour les petits dépôts. Les fichiers plus récents que le début du marquage (`--max-file-modification`) sont protégés, ce qui couvre les écritures en cours.

## Critères

1. **Le stockage ne grossit plus sans limite.**
2. **Aucune donnée vivante supprimée**, sur aucune branche, y compris les écritures en cours.
3. **Un retour arrière de quelques jours** reste possible dans Nessie.
4. **Peu de composants en plus**, des outils de la stack ou de ses projets.
5. **Automatique**, sans intervention.

## Options étudiées

### Nettoyage

| Option | Avantages | Limites |
| --- | --- | --- |
| Nessie GC en `CronJob`, état en mémoire (`--inmemory`) | Un seul conteneur par nuit, aucune base de données. Outil prévu par Nessie, qui tient compte de toutes les branches | Une exécution interrompue recommence depuis le début. Pas de relecture des fichiers avant suppression |
| Nessie GC avec son état dans PostgreSQL | Phases reprenables, suppressions différées et relisibles (`--defer-deletes`) | Une base, un rôle et un Secret de plus, et une manipulation SQL sur le volume PostgreSQL existant |
| `expire_snapshots` et `remove_orphan_files` d'Iceberg | Commandes Trino connues | Ne voient qu'une branche : elles supprimeraient des fichiers encore utilisés par une autre |

### Rétention

| Option | Avantages | Limites |
| --- | --- | --- |
| 7 jours | Une semaine de retour arrière, environ 2,5 Go au plus | Une erreur découverte après une semaine ne se rattrape plus par Nessie |
| 30 jours | Un mois de retour arrière | Environ 10 Go gardés pour un usage rare |

### Compactage

| Option | Avantages | Limites |
| --- | --- | --- |
| `optimize` de Trino sur les tables bronze, chaque nuit, par une macro dbt lancée d'un DAG Airflow | Réutilise la connexion de dbt à Trino et les sources du projet. Pas d'outil nouveau | Le compactage se fait directement sur `main` |
| Pas de compactage | Rien à exploiter | Environ 36 000 petits fichiers par an dans la table temps réel |

## Décision

**Compactage (étape 8.1)** : le DAG Airflow `iceberg_maintenance`, à 02:30 UTC, lance `dbt run-operation optimize_bronze_tables --target dev`. La macro exécute `ALTER TABLE … EXECUTE optimize(file_size_threshold => '64MB')` sur les deux tables bronze, sur `main`, où écrit le writer. Silver et gold, réécrites en entier à chaque exécution du DAG dbt, n'en ont pas besoin.

Le compactage ne passe pas par le Write-Audit-Publish : il ne change pas le contenu des tables, seulement leur découpage en fichiers. Si le compactage et le writer commitent la même table au même moment, Nessie en accepte un et rejette l'autre. Le compactage recommence (deux nouvelles tentatives), ou le writer redémarre et retrouve ses offsets dans les données (ADR 010).

**Nettoyage (étape 8.2)** : un `CronJob` lance chaque nuit, à 04:00 UTC, Nessie GC 0.109.0 (`gc --inmemory --default-cutoff P7D`), après le compactage. Il garde 7 jours d'historique sur toutes les branches, et supprime les fichiers Iceberg qui ne servent plus à aucun commit gardé. Il utilise sa propre identité S3, `nessie-gc` (ADR 006), la seule à supprimer des fichiers en masse, passée par les variables d'environnement `AWS_*` plutôt que dans l'option `--iceberg`, qui l'aurait mise sur la ligne de commande.

## Conséquences

- **Le stockage se stabilise** autour des tables actuelles et de 7 jours de versions.
- **Plus de retour arrière au-delà de 7 jours** dans Nessie. Les données sources restent rechargeables depuis l'API (historique et temps réel).
- **Après un compactage, le dernier snapshot de la table temps réel ne vient pas du writer** : au redémarrage suivant, le writer retrouve ses offsets par `max(_kafka_offset) + 1`, comme prévu par l'ADR 010.
- **Le premier nettoyage supprime d'un coup l'historique de plus de 7 jours** : il se lance à la main et se vérifie (smoke test, `dbt build`, lecture de chaque table).
- **Pour la production** : Nessie GC avec son état dans PostgreSQL et des suppressions différées relues, et des alertes sur la durée et l'échec des exécutions.

## Sources

- Trino 484, `TrinoNessieCatalog` (suppression de table sans suppression de fichiers, renvoi vers Nessie GC). https://github.com/trinodb/trino/blob/484/plugin/trino-iceberg/src/main/java/io/trino/plugin/iceberg/catalog/nessie/TrinoNessieCatalog.java
- Trino, procédure `optimize` du connecteur Iceberg. https://trino.io/docs/current/connector/iceberg.html#optimize
- Nessie 0.109.0, gestion du dépôt et Nessie GC (phases, rétentions, `--inmemory`). https://github.com/projectnessie/nessie/blob/nessie-0.109.0/site/docs/guides/management.md
- Nessie 0.109.0, options de la commande `gc`. https://github.com/projectnessie/nessie/tree/nessie-0.109.0/gc/gc-tool/src/main/java/org/projectnessie/gc/tool/cli/options
