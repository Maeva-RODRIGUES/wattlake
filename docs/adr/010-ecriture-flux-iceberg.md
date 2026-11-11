# ADR 010 : écriture du flux temps réel dans Iceberg

- **Statut** : accepté
- **Date** : novembre 2026

## Contexte

Le producteur (ADR 009) publie les mesures temps réel dans le topic Kafka `eco2mix-realtime` :

- un message par version d'un quart d'heure ;
- clé = le quart d'heure, valeur = la ligne de l'API dans une enveloppe JSON ;
- livraison « au moins une fois » : un même quart d'heure arrive plusieurs fois, à chaque révision et à chaque fenêtre de recouvrement.

Il faut maintenant écrire ces messages dans le lac, au format Iceberg, dans le catalogue Nessie et sur SeaweedFS, comme le reste des tables.

Le volume est très faible : environ 14 messages par quart d'heure, et un rattrapage d'environ 7 000 messages au premier démarrage. La table grandit d'environ 35 000 lignes par an.

## Critères

1. **Exactement une fois dans la table** : un redémarrage ne perd ni ne duplique aucun message.
2. **Historique des versions** : les révisions de RTE restent visibles.
3. **Cohérence avec la stack** : même catalogue (REST de Nessie, ADR 004), même stockage, une identité S3 propre (ADR 006).
4. **Coût d'exploitation** : peu de composants et de mémoire, sur un cluster local.
5. **Explicable** : chaque étape doit pouvoir être lue et défendue.

## Options étudiées

### Outil

| Option | Avantages | Limites |
| --- | --- | --- |
| Consommateur Python avec PyIceberg | Même bibliothèque et même catalogue que le chargement bronze. Environ 200 Mo de mémoire. Toute la sémantique est dans le dépôt | La gestion des offsets est à écrire et à tester soi-même |
| Kafka Connect avec le connecteur Iceberg d'Apache Iceberg, via la ressource `KafkaConnect` de Strimzi | Outil standard. Exactement une fois intégré, grâce à un topic de contrôle | Aucun binaire publié : il faut compiler Iceberg avec Gradle et construire une image Connect, ou laisser Strimzi la construire et la pousser dans un registre. JVM d'environ 1 Go. Ajout seulement, sans upsert. Types déduits d'un JSON sans schéma |
| Spark Structured Streaming ou Flink | Références pour les gros volumes, avec un support complet d'Iceberg | Un opérateur de plus et des JVM de plusieurs Go, pour une centaine de messages par heure |

### Modèle de table

| Option | Avantages | Limites |
| --- | --- | --- |
| Ajout seul : une ligne par message, la couche silver garde la dernière version | Toutes les révisions restent visibles. Chaque écriture est un ajout, sans relecture. Même principe que la couche bronze de l'historique : la donnée brute, sans transformation | Plusieurs lignes par quart d'heure, à dédoublonner en aval |
| Upsert sur `date_heure` | La table donne directement l'état courant | L'historique des révisions est perdu. Chaque lot relit et réécrit des fichiers |

### Reprise après un redémarrage

| Option | Avantages | Limites |
| --- | --- | --- |
| Offsets stockés dans le résumé du snapshot Iceberg, dans le même commit que les données | Données et offsets avancent ensemble ou pas du tout : exactement une fois | Il faut relire le résumé au démarrage. Un snapshot expiré oblige à relire les données |
| Offsets commités dans le groupe de consommateurs Kafka après le commit Iceberg | Mécanisme standard de Kafka | Un arrêt entre les deux commits duplique le dernier lot |

## Décision

**Un consommateur Python, `wattlake_streaming.writer`, tourne dans un `Deployment` à un réplica**, dans la même image que le producteur (`wattlake-streaming`), avec la stratégie `Recreate`.

**Il écrit dans `bronze.eco2mix_national_realtime`, en ajout seul :**

- les 41 champs de l'API, avec leur type d'origine, y compris le nombre typé en texte ;
- `_source_dataset`, `_source_updated_at` (date de rafraîchissement du jeu lue par le producteur), `_kafka_partition`, `_kafka_offset` et `_ingested_at`.

Le format est vérifié message par message : un champ manquant ou inattendu arrête le writer, plutôt que de perdre ou d'ajouter des données en silence. La table n'est pas partitionnée, comme celle de l'historique.

**Chaque lot fait un seul commit Iceberg :**

- un lot dure 5 minutes, ou 10 000 messages au plus, et n'est écrit que s'il contient des messages ;
- le snapshot porte dans son résumé `wattlake.kafka.topic` et `wattlake.kafka.next-offsets`, l'offset suivant de chaque partition.

**Au démarrage, le writer repart de ces offsets.** Il les lit dans le snapshot courant de la table, ou dans le dernier de ses ancêtres qu'il a écrit. Par Nessie, une table n'expose que son snapshot courant, sans ses parents : l'historique est dans les commits Nessie, pas dans la liste des snapshots Iceberg (vérifié dans le cluster, `parent_id` vaut `NULL`). Si le dernier commit sur la table ne vient pas du writer, par exemple une maintenance, il calcule `max(_kafka_offset) + 1` par partition dans les données. Le résultat est le même, en plus lent.

**Les offsets sont aussi commités dans le groupe Kafka `wattlake-writer`, après le commit Iceberg, pour la supervision seulement** : `kafka-consumer-groups.sh` montre alors le retard du writer. Ils ne servent jamais à reprendre.

**Le writer a sa propre identité S3, `streaming`**, ajoutée par `make secrets` dans un nouveau groupe de Secrets. Le script `add_s3_identity.py` l'ajoute à la configuration S3 de SeaweedFS, même sur un cluster existant. Il faut ensuite redémarrer le filer de SeaweedFS.

## Conséquences

- **Exactement une fois entre Kafka et la table** : un message n'y entre qu'une fois, même après un arrêt à n'importe quel moment. La table garde pourtant plusieurs lignes par quart d'heure, qui sont les versions publiées par le producteur. La dernière version d'un quart d'heure est la ligne au `_source_updated_at` le plus récent, et, à égalité, au `_kafka_offset` le plus grand.
- **Environ 100 commits par jour**, donc autant de commits Nessie et de petits fichiers Parquet. Il faudra une maintenance : compactage des fichiers, et nettoyage des fichiers que plus aucun commit Nessie ne référence (le GC de Nessie, puisque c'est lui qui garde l'historique). Après un compactage, le writer retrouve ses offsets dans les données.
- **Une seule table bronze par source** : l'historique (`eco2mix_national_history`) et le temps réel (`eco2mix_national_realtime`) restent séparés. Leur union et le dédoublonnage reviennent aux modèles dbt de la couche silver.
- **Un rattrapage complet tient en un lot** : environ 7 000 lignes. Si le topic grossit au-delà de 10 000 messages en attente, le rattrapage se fait en plusieurs commits, chacun cohérent avec ses offsets.
- **Je réexaminerai le choix de Kafka Connect** si le nombre de topics ou de tables augmente : un connecteur configuré remplace alors du code par table.

## Sources

- Apache Iceberg 1.12.0, connecteur Kafka Connect : construction depuis les sources (« The connector zip archive is created as part of the Iceberg build »), exactement une fois, paramètres. https://github.com/apache/iceberg/blob/apache-iceberg-1.12.0/docs/docs/kafka-connect.md
- PyIceberg 0.12.0, `Table.append` et son paramètre `snapshot_properties` (« Custom properties to be added to the snapshot summary »). https://github.com/apache/iceberg-python/blob/pyiceberg-0.12.0/pyiceberg/table/__init__.py
- Spécification Iceberg, résumé des snapshots (`summary`). https://iceberg.apache.org/spec/#snapshots
- Nessie 0.109.0, catalogue REST Iceberg (« Nessie will always return only the Iceberg table snapshot that corresponds to the Nessie commit »). https://github.com/projectnessie/nessie/blob/nessie-0.109.0/site/docs/guides/iceberg-rest.md
