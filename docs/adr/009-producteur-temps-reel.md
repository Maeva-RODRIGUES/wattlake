# ADR 009 : producteur temps réel vers Kafka

- **Statut** : accepté
- **Date** : novembre 2026

## Contexte

Le jeu éCO2mix temps réel (`eco2mix-national-tr`, voir [docs/sources.md](../sources.md)) couvre les mesures depuis le 1ᵉʳ septembre et les prévisions jusqu'au lendemain. Il est rafraîchi tous les quarts d'heure, et la dernière mesure a environ 15 minutes de retard. Le chargement batch s'arrête au 31 août, avec l'historique consolidé : rien dans le lac ne couvre la période depuis.

L'étape 5 fait passer ces données par Kafka (déployé à l'étape 5.1) avant de les écrire dans Iceberg (étape 5.3). Cet ADR fixe comment elles entrent dans Kafka. L'outil qui les écrira dans Iceberg fera l'objet de son propre ADR.

Contraintes de la source :

- L'API ne pousse rien : il faut l'interroger.
- Le quota est de 50 000 appels par mois sur un même jeu de données.
- RTE peut réviser une mesure récente : un quart d'heure déjà lu peut changer.
- `/records` ne lit que 10 000 lignes, `/exports` n'a pas de limite.

## Critères

1. **Rien de perdu** : un redémarrage du producteur ou de Kafka ne laisse pas de trou.
2. **Révisions transmises** : une valeur corrigée par RTE arrive dans le flux.
3. **Peu de pièces** : pas de base d'état ni de composant de plus à exploiter.
4. **Format lisible et stable** pour le consommateur de l'étape 5.3.
5. **Quota respecté**, avec de la marge.

## Options étudiées

### Où tourne le producteur

| Option | Avantages | Limites |
| --- | --- | --- |
| Un `Deployment` Kubernetes, processus Python qui boucle | Forme normale d'un producteur : il vit en continu, Kubernetes le redémarre s'il tombe. Image légère et indépendante d'Airflow | Une image et un déploiement de plus |
| Une tâche Airflow toutes les 15 minutes | Pas de nouvelle image, historique des exécutions dans Airflow | Du micro-batch déguisé. Mélange orchestration et flux. Dépend des contraintes de versions d'Airflow et de son image d'environ 1,5 Go |

### Format des messages

| Option | Avantages | Limites |
| --- | --- | --- |
| JSON, schéma documenté dans le dépôt | Lisible avec les outils de Kafka, aucun composant de plus | Rien n'empêche un producteur d'envoyer un autre schéma |
| Avro avec un Schema Registry | Schéma vérifié à l'écriture, évolutions contrôlées | Un registre à déployer et à exploiter, pour un seul producteur et un seul consommateur |

### Reprise après un redémarrage

| Option | Avantages | Limites |
| --- | --- | --- |
| Lire le dernier message du topic au démarrage | Kafka est la seule source de vérité : pas d'état ailleurs | Un appel de plus au démarrage |
| Stocker le curseur ailleurs (ConfigMap, fichier sur un volume, base) | Lecture simple | Un état de plus, qui peut diverger du topic si une publication échoue après son écriture |

### Révisions de RTE

| Option | Avantages | Limites |
| --- | --- | --- |
| Republier une fenêtre de recouvrement à chaque rafraîchissement, sur un topic compacté | Une valeur révisée dans la fenêtre est republiée. La compaction ne garde que la dernière version de chaque quart d'heure | Le consommateur reçoit plusieurs versions d'un même quart d'heure et doit garder la plus récente |
| Ne publier que les nouveaux quarts d'heure | Un message par quart d'heure | Une révision n'est jamais transmise |

## Décision

**Un `Deployment` à un réplica fait tourner le producteur `wattlake_streaming`** ([streaming/](../../streaming/)), dans sa propre image `python:3.13.17-slim`, avec le client `confluent-kafka` (librdkafka). La stratégie `Recreate` garantit qu'il n'y a jamais deux producteurs à la fois.

**Chaque cycle, toutes les 5 minutes :**

1. Le producteur lit la date de rafraîchissement du jeu (`data_processed`). Si elle n'a pas changé, il s'arrête là.
2. Sinon, il exporte les lignes mesurées (`consommation is not null`) depuis son curseur moins 3 heures, triées par quart d'heure.
3. Il les publie dans l'ordre et attend que Kafka les ait toutes acceptées, en producteur idempotent (`enable.idempotence`, donc `acks=all`).
4. Il avance alors son curseur au dernier quart d'heure publié.

Un rafraîchissement est publié au plus 5 minutes après RTE, pour environ 8 600 appels par mois, plus un export par rafraîchissement.

**Au démarrage, le curseur est la clé du dernier message du topic.** Le topic vide, il publie tout le jeu, en un seul export : environ 6 900 lignes en 2 secondes.

**Les messages sont en JSON :**

- clé : le quart d'heure (`date_heure` en UTC, tel que publié par l'API) ;
- valeur : `{"dataset", "source_updated_at", "record"}`, où `record` est la ligne de l'API sans aucune modification, et `source_updated_at` la date de rafraîchissement du jeu à la lecture. Le nettoyage reste le travail de la couche silver.

**Le topic `eco2mix-realtime`** a une partition, qui garde tous les quarts d'heure dans l'ordre, et `cleanup.policy=compact`. Il est déclaré par un `KafkaTopic` ([infra/kafka/topics.yaml](../../infra/kafka/topics.yaml)).

## Conséquences

- **La livraison est « au moins une fois »** : un même quart d'heure arrive plusieurs fois (fenêtre de recouvrement, cycle rejoué après une erreur). Le consommateur de l'étape 5.3 doit écrire par clé (un `MERGE` sur `date_heure`) et garder la version au `source_updated_at` le plus récent.
- **Pas de trou après une panne.** Un cycle interrompu en cours de publication reprend depuis le dernier message livré, moins 3 heures. Une panne de l'API saute le cycle, et le suivant rattrape tout depuis le curseur.
- **Une révision plus vieille que 3 heures n'est pas transmise.** Les corrections plus tardives arrivent avec les données consolidées, que le batch charge depuis l'historique.
- **Le topic compacté est un miroir dédoublonné du jeu temps réel** : relu depuis le début, il redonne la dernière version de chaque quart d'heure, sans limite de durée. Le jeu ne grossit que d'environ 35 000 lignes par an.
- **Les prévisions ne passent pas par Kafka** : seules les lignes mesurées sont publiées.
- **Rien ne vérifie le schéma des messages.** Je réexaminerai le choix d'un Schema Registry si d'autres producteurs ou consommateurs s'ajoutent.

## Sources

- Opendatasoft Explore API v2.1 : `/exports` sans limite de lignes, `/records` limité à 10 000. https://help.opendatasoft.com/apis/ods-explore-v2/
- Kafka 4.3, compaction des logs (« Log compaction ensures that Kafka will always retain at least the last known value for each message key within the log of data for a single topic partition »). https://kafka.apache.org/43/design/design/
- Kafka 4.3, producteur idempotent et `acks=all`. https://kafka.apache.org/43/configuration/producer-configs/
- confluent-kafka-python 2.16.1. https://pypi.org/project/confluent-kafka/2.16.1/
