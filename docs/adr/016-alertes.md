# ADR 016 : alertes

- **Statut** : accepté
- **Date** : novembre 2026

## Contexte

La supervision (ADR 011) collecte les métriques et les logs, mais n'alerte personne : un problème ne se voit qu'en ouvrant Grafana. Les pannes déjà rencontrées seraient passées inaperçues des heures durant :

- **le flux temps réel arrêté** (producteur bloqué, API indisponible) : le retard du writer reste alors à 0, et le tableau de bord ne montre rien d'anormal ;
- **silver et gold non publiés** : quand un test dbt échoue, le Write-Audit-Publish (ADR 008) garde la dernière version testée sur `main`. C'est voulu, mais rien ne prévient que les données vieillissent. Par exemple, chaque matin après la veille du portable, la veille est incomplète (le flux s'est arrêté pendant la nuit), le test `assert_incomplete_days_are_known` échoue, et `dbt_transformations` ne publie pas jusqu'à ce que le producteur ait rattrapé la nuit (vérifié dans Airflow les 19, 20 et 21 novembre 2026).

Ce qui existe déjà, vérifié dans le cluster :

- **Prometheus évalue les 30 groupes de règles par défaut de `kube-prometheus-stack`**, sans Alertmanager pour les recevoir. Seules deux alertes sont actives : `Watchdog`, qui se déclenche toujours par construction, et `PrometheusNotConnectedToAlertmanagers`. Les règles des composants que k3s n'expose pas (contrôleur, ordonnanceur, proxy, etcd) ne sont pas créées, puisque leur collecte est désactivée.
- **Les métriques utiles aux pipelines sont déjà collectées** : `kafka_consumergroup_lag` et `kafka_topic_partition_current_offset` (Kafka Exporter), `airflow_dagrun_duration_count` et `airflow_dagrun_failed_count` (exporter StatsD d'Airflow, qui compte les runs réussis et les runs en échec, avec le label `dag_id`).

## Critères

1. **Déclaratif et versionné** : les règles vivent dans le dépôt, déployées par Argo CD.
2. **Testable** : une règle se vérifie avant d'être déployée, sur des séries simulées.
3. **Peu de composants en plus.**
4. **Pas de fausse alerte prévisible** : redémarrage d'un exporter, réveil du portable.
5. **Standard du métier.**

## Options étudiées

### Moteur d'alertes

| Option | Avantages | Limites |
| --- | --- | --- |
| Règles Prometheus (`PrometheusRule`) et Alertmanager | Les règles sont des ressources Kubernetes, versionnées à côté du reste. Testables avec `promtool test rules`. Alertmanager regroupe, inhibe et met en sourdine. Déjà fourni par le chart, avec les règles par défaut | Un pod de plus |
| Alertes gérées par Grafana | Une seule interface ; peut aussi interroger Loki | Règles stockées dans la base de Grafana (sans volume ici, ADR 011) ou provisionnées dans un format propre à Grafana ; pas de test unitaire ; les règles par défaut du chart resteraient sans destinataire |

### Signal de « silver et gold publiés »

| Option | Avantages | Limites |
| --- | --- | --- |
| Runs réussis de `dbt_transformations` (`airflow_dagrun_duration_count`) | Déjà collecté. Un run réussi est un run publié : `publish` est sa dernière tâche | Compteur remis à zéro au redémarrage de l'exporter StatsD |
| Métrique poussée par la tâche `publish` (Pushgateway) | Horodatage exact de la dernière publication | Un composant de plus, et du code dans le DAG |
| Fraîcheur lue dans les tables (requête Trino) | Mesure la donnée elle-même | Il faudrait un exporter SQL à déployer et à maintenir |

## Décision

**Alertmanager est activé dans `kube-prometheus-stack`** ([infra/monitoring/values.yaml](../../infra/monitoring/values.yaml)) :

- les alertes sont regroupées par nom et par namespace, et renotifiées toutes les 12 heures au plus ;
- `Watchdog` n'est jamais notifiée : elle prouve seulement que la chaîne d'alerte fonctionne ;
- une alerte `critical` met en sourdine les alertes `warning` de même nom et de même namespace ;
- aucun canal de notification pour l'instant : les alertes se lisent dans l'interface d'Alertmanager et dans Grafana, qui reçoit Alertmanager comme source de données.

**Les règles par défaut du chart sont gardées telles quelles** : elles couvrent la plateforme (`TargetDown`, `KubePodCrashLooping`, `KubePersistentVolumeFillingUp`…), et aucune ne se déclenche à tort aujourd'hui.

**Cinq règles WattLake**, dans une `PrometheusRule` déployée par l'Application `monitoring` ([infra/monitoring/rules/wattlake-alerts.yaml](../../infra/monitoring/rules/wattlake-alerts.yaml)) :

| Alerte | Condition | Gravité |
| --- | --- | --- |
| `WattlakeWriterLagging` | Des messages attendent le writer depuis plus de 15 minutes (un lot dure 5 minutes) | warning |
| `WattlakeRealtimeStreamStalled` | Aucun nouveau message dans le topic depuis 1 h 30 (RTE rafraîchit le jeu environ tous les quarts d'heure) | warning |
| `WattlakeKafkaMetricsMissing` | Plus de série `kafka_consumergroup_lag` pour le writer depuis 15 minutes : les deux règles précédentes seraient aveugles | warning |
| `WattlakeDagRunFailed` | Un run de DAG a échoué dans l'heure | warning |
| `WattlakeDbtNotPublished` | Aucun run réussi de `dbt_transformations` depuis 2 heures, pendant 15 minutes | critical |

**Deux précautions contre les compteurs StatsD**, qui n'existent qu'après leur premier événement et repartent de zéro quand l'exporter redémarre :

- `WattlakeDagRunFailed` ajoute à `increase()` les séries apparues dans l'heure (`unless … offset 1h`) : sans cela, le premier échec depuis le démarrage de l'exporter passerait inaperçu, car `increase()` a besoin de deux points ;
- `WattlakeDbtNotPublished` n'alerte que si la série existait déjà 2 heures plus tôt, pour qu'un redémarrage de l'exporter (une série neuve, avec un seul point) ne passe pas pour une publication manquante. `absent_over_time` couvre le cas où aucun run n'a réussi depuis ce redémarrage.

## Conséquences

- **`WattlakeDagRunFailed` se déclenche presque chaque matin**, au réveil du portable : la veille est incomplète et le Write-Audit-Publish refuse de publier. L'alerte est juste : les données sont réellement incomplètes. Elle se résout seule une heure après le dernier échec. `WattlakeDbtNotPublished` ne se déclenche pas dans ce cas, parce que Prometheus n'a pas de points pendant la veille (`offset 2h` vide).
- **Les sourdines sont perdues au redémarrage d'Alertmanager**, qui n'a pas de volume. C'est acceptable sur un cluster local.
- **Environ 30 Mio de mémoire en plus.**
- **Les règles restent à tester** sur des séries simulées, avec `promtool test rules`, et l'envoi des alertes reste à brancher sur un canal de notification.

## Sources

- kube-prometheus-stack 92.14.1, valeurs `alertmanager.config` et `alertmanager.alertmanagerSpec`. https://github.com/prometheus-community/helm-charts/tree/kube-prometheus-stack-92.14.1/charts/kube-prometheus-stack
- Prometheus, règles d'alerte et fonctions `increase`, `absent`, `absent_over_time`. https://prometheus.io/docs/prometheus/latest/configuration/alerting_rules/
- Alertmanager, routage et inhibition. https://prometheus.io/docs/alerting/latest/configuration/
- Chart Airflow 1.22.0, correspondances StatsD (`dagrun.duration.success.*` vers `airflow_dagrun_duration`, `dagrun.duration.failed.*` vers `airflow_dagrun_failed`), lues dans le ConfigMap `airflow-statsd` du cluster.
