# ADR 011 : supervision de la stack (métriques et logs)

- **Statut** : accepté
- **Date** : novembre 2026

## Contexte

Jusqu'ici, rien n'est collecté :

- pour savoir si le flux temps réel est à jour, il faut lire les logs du writer ou interroger la table ;
- les logs ne sont lisibles qu'avec `kubectl logs`, et disparaissent quand un pod est recréé ;
- les problèmes rencontrés (redémarrages du dag-processor, pannes DNS) se sont diagnostiqués à la main.

L'étape 6.1 ajoute des métriques et des logs centralisés.

Ce que les composants exposent déjà, vérifié dans le cluster :

| Composant | Métriques |
| --- | --- |
| Nessie 0.109.0 | Endpoint Prometheus `/q/metrics` sur le port de gestion (9000). Le chart crée un `ServiceMonitor` dès que la CRD existe |
| Trino 484 | Endpoint OpenMetrics `/metrics` sur le port HTTP. Il exige un nom d'utilisateur (401 sans), accepte HTTP Basic avec un mot de passe vide. Le chart 1.45.0 crée le `ServiceMonitor` |
| Kafka (Strimzi 1.3.0) | `metricsConfig: strimziMetricsReporter` sur les brokers, et Kafka Exporter pour le retard des groupes de consommateurs (port `tcp-prometheus`) |
| Airflow 3.2.2 | Métriques StatsD, converties par l'exporter StatsD du chart 1.22.0 |

Le cluster est local, avec un seul développeur, et assez de mémoire (environ 6 Gio utilisés).

## Critères

1. **Déclaratif** : ce qui est collecté est décrit dans le dépôt, à côté de chaque composant.
2. **Métriques et logs dans le même outil.**
3. **Coût** : quelques Gio de mémoire au plus, peu de composants à exploiter.
4. **Standards du métier**, que l'on retrouve en entreprise.
5. **Aucun mot de passe par défaut** (ADR 006).

## Options étudiées

### Métriques

| Option | Avantages | Limites |
| --- | --- | --- |
| `kube-prometheus-stack` | Opérateur Prometheus et CRD `ServiceMonitor` / `PodMonitor` : chaque chart déclare sa collecte. Grafana, node-exporter, kube-state-metrics, règles et tableaux de bord Kubernetes inclus | Beaucoup de ressources installées, dont des CRD volumineuses |
| Charts `prometheus` et `grafana` séparés | Plus léger | Pas de CRD : toute la collecte dans une configuration centrale, écrite à la main. Les `ServiceMonitor` des charts Nessie et Trino sont inutilisables |

### Logs

| Option | Avantages | Limites |
| --- | --- | --- |
| Loki en mode `SingleBinary` sur un volume, alimenté par Alloy | Un pod Loki, un pod Alloy. Logs lisibles dans Grafana, avec les mêmes labels que les métriques | Un volume local : pas de haute disponibilité |
| Loki sur SeaweedFS (S3) | Proche d'une installation de production, Loki peut passer à l'échelle | Un bucket et une identité S3 de plus, pour un gain nul en local |
| Elasticsearch ou OpenSearch, avec Fluent Bit | Recherche plein texte complète | Une JVM de plusieurs Go, un second outil de visualisation |

### Collecte des logs par Alloy

| Option | Avantages | Limites |
| --- | --- | --- |
| Un `Deployment`, qui lit les logs par l'API Kubernetes (`loki.source.kubernetes`) | Un seul pod, aucun répertoire de l'hôte monté | L'API Kubernetes porte tout le trafic de logs : suffisant pour quelques dizaines de pods |
| Un `DaemonSet`, qui lit `/var/log/pods` sur chaque nœud | Ne charge pas l'API, passe à l'échelle | Un pod par nœud, et un montage `hostPath` |

## Décision

**Trois Applications Argo CD, dans le namespace `monitoring` :**

- **`monitoring`** : `kube-prometheus-stack` 92.14.1. Prometheus garde 7 jours de métriques (4 Go au plus, volume de 5 Gi). Grafana a un mot de passe admin dans le Secret `grafana-admin`. Alertmanager est désactivé, tout comme les moniteurs des composants que k3s n'expose pas (contrôleur, ordonnanceur, proxy, etcd).
- **`loki`** : Loki 7.5.0 en mode `SingleBinary`, stocké sur un volume de 5 Gi, avec 7 jours de rétention appliqués par le compacteur. Sans gateway, sans caches memcached, sans canary.
- **`alloy`** : Alloy 1.14.4 en `Deployment`, qui lit les logs de tous les pods par l'API Kubernetes et les envoie à Loki, avec les labels `namespace`, `pod`, `container` et `app`.

**Prometheus prend tous les `ServiceMonitor` et `PodMonitor` du cluster**, pas seulement ceux de sa release Helm :

- **Nessie et Trino** : créés par leurs charts. Celui de Trino s'authentifie sous l'utilisateur `prometheus`, sans mot de passe.
- **Kafka** : un `PodMonitor` sur les pods `strimzi.io/kind=Kafka` (brokers et Kafka Exporter).
- **Airflow** : un `ServiceMonitor` sur l'exporter StatsD du chart, activé pour l'occasion.

**Un tableau de bord « WattLake » est versionné** dans `infra/monitoring/dashboards/`, et chargé par le sidecar de Grafana. Il montre :

- **le flux temps réel** : retard du groupe `wattlake-writer`, messages publiés par heure ;
- **Airflow** : exécutions de DAG réussies et en échec, durée moyenne ;
- **Trino** : requêtes terminées et en échec ;
- **Nessie** : opérations du version store ;
- **les logs** du producteur et du writer.

## Conséquences

- **Le retard du writer devient l'indicateur de fraîcheur du flux** : il doit revenir à 0 au plus 5 minutes après chaque rafraîchissement de RTE.
- **Environ 1,5 à 2 Gio de mémoire en plus.**
- **Modifier les paramètres de Kafka redémarre le broker**, pour les métriques comme pour le reste. Le producteur et le writer reprennent seuls (ADR 009 et 010).
- **Les tableaux de bord se modifient dans le dépôt.** Une modification faite dans l'interface de Grafana est perdue au redémarrage : Grafana n'a pas de volume.
- **Aucune alerte pour l'instant.** En production, j'activerais Alertmanager, avec des règles sur le retard du writer, les échecs de DAG et la disponibilité des composants, et je stockerais Loki dans un stockage objet.

## Sources

- kube-prometheus-stack 92.14.1, valeurs par défaut (`serviceMonitorSelectorNilUsesHelmValues`, `grafana.admin.existingSecret`, `additionalPodMonitors`). https://github.com/prometheus-community/helm-charts/tree/kube-prometheus-stack-92.14.1/charts/kube-prometheus-stack
- Loki, installation monolithique (`SingleBinary`). https://grafana.com/docs/loki/latest/setup/install/helm/install-monolithic/
- Alloy, composant `loki.source.kubernetes`. https://grafana.com/docs/alloy/latest/reference/components/loki/loki.source.kubernetes/
- Strimzi 1.3.0, exemples de métriques (`strimziMetricsReporter`, `PodMonitor` sur `tcp-prometheus`). https://github.com/strimzi/strimzi-kafka-operator/tree/1.3.0/packaging/examples/metrics
- Trino, métriques OpenMetrics. https://trino.io/docs/current/admin/openmetrics.html
