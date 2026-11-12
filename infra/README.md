# infra

Déploiement de la stack sur Kubernetes : configuration du cluster k3d et vérification de son DNS (`k3d/`), valeurs des charts Helm (SeaweedFS, Nessie, Trino, Airflow, opérateur Strimzi, supervision : `monitoring/`, `loki/`, `alloy/`), ressources du cluster Kafka et ses topics (`kafka/`, kustomize), flux temps réel, producteur et writer (`streaming/`, kustomize), manifestes de PostgreSQL (`postgres/`, kustomize), Applications Argo CD (`argocd/`) et script de création des Secrets (`secrets/`).

## GitOps avec Argo CD

Argo CD est le seul composant installé à la main (`make argocd-up`, chart `argo/argo-cd` 10.11.1, valeurs dans [argocd/values.yaml](argocd/values.yaml)). Il déploie tous les autres depuis la branche `main` du dépôt GitHub :

- [argocd/root.yaml](argocd/root.yaml) : l'Application racine (« app of apps »), appliquée par `make argocd-apps`. Elle déploie les Applications de [argocd/apps/](argocd/apps/).
- Une Application par composant : SeaweedFS, Nessie, Trino, Airflow et l'opérateur Strimzi combinent leur chart, à la version épinglée, et le fichier de valeurs de ce dossier. PostgreSQL et Kafka déploient les dossiers kustomize [postgres/](postgres/) et [kafka/](kafka/).

Le cluster suit donc ce qui est sur `main` : un changement de valeurs ou de version passe par un commit, pas par une commande `helm`.

## Reprise des composants installés par Helm

Avant Argo CD, les composants étaient installés par `helm upgrade --install` depuis le Makefile. Chaque Application garde le nom de la release Helm (`releaseName`), donc les mêmes noms d'objets : Argo CD reprend les objets existants au lieu d'en créer de nouveaux.

Avant la reprise, chaque chart a été rendu avec `helm template` puis comparé au cluster avec `kubectl diff`, en lecture seule. Résultat :

- **Nessie, Trino, SeaweedFS et PostgreSQL** : aucun objet en fonctionnement modifié. Seuls apparaissent les pods de test Helm (`*-test-connection`) et le Job qui crée le bucket `warehouse`, qui vérifie d'abord que le bucket existe. Le Deployment de Nessie est identique : la reprise ne le redémarre pas, ce qui comptait alors, son catalogue étant en mémoire jusqu'à l'étape 4.5.
- **Airflow** : nouvelle image (ghcr.io, tag du commit) et nouveaux Jobs de migration et de création d'utilisateur. Les pods d'Airflow redémarrent, sans perte : les métadonnées sont dans PostgreSQL.

La reprise s'est faite en synchronisation manuelle, composant par composant. L'écart calculé par Argo CD a confirmé la mesure faite avec `kubectl diff` : pour PostgreSQL, SeaweedFS, Trino et Nessie, la seule différence était l'annotation de suivi d'Argo CD (`argocd.argoproj.io/tracking-id`), et aucun pod n'a redémarré. Les métadonnées des anciennes releases Helm (secrets `sh.helm.release.v1.*`) ont ensuite été supprimées : un `helm uninstall` aurait sinon effacé des objets désormais gérés par Argo CD.

## Synchronisation automatique

Toutes les Applications sont en synchronisation automatique, avec `prune` (ce qui disparaît de Git est supprimé du cluster) et `selfHeal` (un changement fait à la main dans le cluster est annulé). Argo CD vérifie le dépôt toutes les trois minutes environ.

L'Application racine elle-même n'est pas lue depuis Git : elle est appliquée par `make argocd-apps`. Une modification de [argocd/root.yaml](argocd/root.yaml) se déploie en relançant cette cible ; une modification des Applications de [argocd/apps/](argocd/apps/) se déploie toute seule.

## Airflow et Argo CD

Le chart Airflow demande, avec Argo CD, `useHelmHooks: false` et `applyCustomEnv: false` sur les Jobs de migration et de création d'utilisateur. Un Job ne se modifie pas une fois créé : l'annotation `argocd.argoproj.io/hook: Sync` fait recréer ces Jobs à chaque synchronisation au lieu de les modifier (voir [airflow/values.yaml](airflow/values.yaml)).

## Secrets

Les identifiants ne sont pas dans le dépôt (voir l'[ADR 006](../docs/adr/006-gestion-des-secrets.md)). `make secrets`, lancé par `make up` avant Argo CD, exécute [secrets/create.sh](secrets/create.sh), qui crée huit Secrets avec des valeurs aléatoires : `seaweedfs-s3-config`, `nessie-credentials`, `trino-credentials`, `postgres-credentials`, `airflow-credentials`, `nessie-db-credentials` et `streaming-credentials` dans le namespace `wattlake`, et `grafana-admin` dans le namespace `monitoring`. Les fichiers de valeurs et les manifestes ne citent que leurs noms.

- **Une identité S3 par composant** : Nessie, Trino, Airflow et le writer temps réel (`streaming`) ont chacun leurs clés, limitées à la lecture, l'écriture et la liste du bucket `warehouse`. L'identité `admin`, utilisée par aucun composant, sert à l'exploitation.
- **Par groupes, en tout ou rien** : des Secrets qui partagent des valeurs forment un groupe (chaque paire de clés S3 est aussi dans la configuration de SeaweedFS). Un groupe est créé en entier ou pas du tout : le script ne remplace jamais un Secret existant, et s'arrête si seuls certains Secrets d'un groupe existent. Un groupe ajouté plus tard se crée sur un cluster existant sans toucher aux autres. Quatre groupes :
  - `stack` : les cinq premiers Secrets ;
  - `nessie-db` : le rôle PostgreSQL de Nessie (voir l'[ADR 007](../docs/adr/007-stockage-versions-nessie.md)) ;
  - `streaming` : les clés du writer temps réel (ADR 010). Le script les ajoute aussi comme identité dans `seaweedfs-s3-config`, par [secrets/add_s3_identity.py](secrets/add_s3_identity.py), sans toucher aux autres identités. Sur un cluster existant, il faut ensuite redémarrer le filer de SeaweedFS, qui ne lit ses identités qu'au démarrage : le script affiche la commande.
  - `monitoring` : le compte admin de Grafana (ADR 011), dans le namespace `monitoring`, que le script crée au besoin.
- **Hors d'Argo CD** : les Secrets ne sont pas suivis par Argo CD, et portent des noms qu'aucun chart ne génère. Argo CD ne peut donc pas les supprimer.

Lors du passage à ces Secrets, Argo CD a supprimé (`prune`) les Secrets que les charts ne généraient plus, sauf deux : `airflow-fernet-key` et `seaweedfs-s3-secret`. Les charts les déclarent comme hooks Helm (annotation `helm.sh/hook`), et Argo CD traite un hook à part : il ne le supprime jamais par `prune`. Ils ont été supprimés à la main. Ce piège vaut pour toute ressource déclarée en hook par un chart.

Lister les Secrets, ou lire une valeur :

```bash
kubectl --context k3d-wattlake -n wattlake get secrets -l app.kubernetes.io/part-of=wattlake
kubectl --context k3d-wattlake -n wattlake get secret airflow-credentials \
  -o jsonpath='{.data.admin-password}' | base64 --decode
```

### Changer les valeurs

1. Supprimer tous les Secrets du groupe à changer, puis relancer `make secrets`.
2. PostgreSQL ne lit ses mots de passe qu'à l'initialisation d'un volume vide. Sur un volume existant, appliquer les nouveaux mots de passe avec `ALTER ROLE`, en psql dans le pod (connexion locale, sans mot de passe). Le SQL est envoyé sur l'entrée standard, pour que les mots de passe n'apparaissent pas dans une ligne de commande :

   ```bash
   S="kubectl --context k3d-wattlake -n wattlake"
   PG=$($S get secret postgres-credentials -o jsonpath='{.data.postgres-password}' | base64 --decode)
   AF=$($S get secret postgres-credentials -o jsonpath='{.data.airflow-password}' | base64 --decode)
   NE=$($S get secret nessie-db-credentials -o jsonpath='{.data.password}' | base64 --decode)
   printf "ALTER ROLE postgres PASSWORD '%s';\nALTER ROLE airflow PASSWORD '%s';\nALTER ROLE nessie PASSWORD '%s';\n" \
     "$PG" "$AF" "$NE" | $S exec -i postgres-0 -- psql --username postgres --set ON_ERROR_STOP=1
   ```

   Ne garder que les lignes des rôles dont le mot de passe a changé.

3. Redémarrer les pods qui lisent ces valeurs : `$S rollout restart` sur les Deployments et StatefulSets de SeaweedFS (filer), Nessie, Trino et Airflow. Un pod ne relit pas ses variables d'environnement quand un Secret change.

Changer la clé Fernet rend illisibles les connexions et variables déjà chiffrées par Airflow. Le catalogue de Nessie est dans PostgreSQL (base `nessie`) : redémarrer Nessie ne le perd pas.

## PostgreSQL

Une seule instance, avec une base par composant, chacune propriété de son rôle :

- `airflow` : les métadonnées d'Airflow (voir l'[ADR 003](../docs/adr/003-base-metadonnees-airflow.md)) ;
- `nessie` : le version store de Nessie, en JDBC2 (voir l'[ADR 007](../docs/adr/007-stockage-versions-nessie.md)). Nessie crée ses tables (`refs2`, `objs2`) au premier démarrage.

Chaque base n'accepte que les connexions de son propriétaire (`REVOKE CONNECT ... FROM PUBLIC`) : le rôle `nessie` ne peut pas se connecter à la base `airflow`, ni l'inverse. Les rôles et les bases sont créés par les scripts de [postgres/initdb.yaml](postgres/initdb.yaml), qui ne s'exécutent que sur un volume vide.

## Kafka

Kafka est géré par l'opérateur **Strimzi** : on décrit le cluster dans des ressources Kubernetes (`Kafka`, `KafkaNodePool`, `KafkaTopic`), et l'opérateur crée et maintient les pods, les volumes, les Services et la configuration.

- **Opérateur** : chart `strimzi-kafka-operator` 1.3.0 (12 octobre 2026 ; la 1.3.1 avait moins de deux semaines), dans le namespace `strimzi`, valeurs dans [strimzi/values.yaml](strimzi/values.yaml). Il ne surveille que le namespace `wattlake` : ses droits y sont limités par des RoleBindings, et ses seuls droits à l'échelle du cluster portent sur les nœuds, les classes de stockage et les ClusterRoleBindings.
- **Cluster** : [kafka/](kafka/), Kafka 4.3.1 en mode **KRaft**, sans ZooKeeper. Un seul nœud porte les rôles de contrôleur et de broker, avec un facteur de réplication de 1 et un volume persistant de 5 Gi (`local-path`) : les messages survivent à un redémarrage du pod ou du cluster.
- **Accès** : `wattlake-kafka-bootstrap.wattlake.svc:9092`, listener interne sans TLS ni authentification, comme Trino et Nessie en local. En production, il faudrait un listener TLS et une authentification (SCRAM ou mTLS), avec le User Operator de Strimzi pour gérer les comptes.
- **Topics** : créés seulement par des ressources `KafkaTopic` versionnées ([kafka/topics.yaml](kafka/topics.yaml)), via le Topic Operator (`auto.create.topics.enable: false`). Une faute de frappe dans un producteur ne crée pas de topic. Seul topic pour l'instant : `eco2mix-realtime`, compacté, alimenté par le producteur temps réel.

Deux réglages d'Argo CD sont propres à Strimzi :

- l'Application `strimzi` utilise `ServerSideApply=true` : la CRD `Kafka` fait environ 350 Ko, au-delà des 256 Ko de l'annotation `last-applied-configuration` qu'utilise l'application côté client ;
- l'Application `kafka` utilise `SkipDryRunOnMissingResource=true` et des nouvelles tentatives : au premier déploiement, ses ressources peuvent être synchronisées avant que l'opérateur ait installé leurs CRD.

## Flux temps réel

[streaming/](streaming/) déploie les deux composants du flux ([../streaming/README.md](../streaming/README.md)), synchronisés par l'Application Argo CD `streaming` depuis la branche `main` :

- [producer.yaml](streaming/producer.yaml) : le producteur, de l'API vers Kafka (ADR 009) ;
- [writer.yaml](streaming/writer.yaml) : le writer, de Kafka vers la table Iceberg `bronze.eco2mix_national_realtime` (ADR 010).

Les deux tournent dans la même image, chacun dans un `Deployment` à un réplica, en stratégie `Recreate` : deux producteurs publieraient chaque ligne deux fois, deux writers l'écriraient deux fois.

- **Sécurité** : utilisateur non root (10001), système de fichiers en lecture seule, toutes les capacités Linux retirées. Seul `/tmp` est accessible en écriture (un `emptyDir`), pour le fichier de heartbeat. Le writer a sa propre identité S3, `streaming` (voir « Secrets »).
- **Sonde de vie** : aucun des deux n'expose de port. Chacun touche un fichier à chaque cycle de 5 minutes, et la sonde (`python -m wattlake_streaming.health`) échoue si ce fichier a plus de 15 minutes. Une panne de l'API ne la déclenche pas : le fichier est touché même quand le cycle est sauté.
- **Ressources** : 256 Mi au plus pour le producteur, dont le premier démarrage monte à environ 85 Mi ; 512 Mi pour le writer, qui charge PyArrow et écrit tout le topic au premier lot.
- **Image** : `ghcr.io/maeva-rodrigues/wattlake-streaming`, publiée par la CI, avec le SHA du commit comme tag, fixé dans [streaming/kustomization.yaml](streaming/kustomization.yaml). Comme pour Airflow, déployer une nouvelle version, c'est changer ce tag dans un commit à part.

Pour essayer une modification avant de la commiter, on peut construire l'image en local, la charger dans k3d, puis l'appliquer à la main avec le tag `local`. Argo CD remet l'image de `main` à la synchronisation suivante, à cause de `selfHeal` : il faut donc suspendre la synchronisation automatique de l'Application `streaming` le temps de l'essai.

```bash
make streaming-image streaming-image-import
```

## Supervision

Métriques et logs de la stack ([ADR 011](../docs/adr/011-supervision.md)), dans le namespace `monitoring`, en trois Applications Argo CD :

- **`monitoring`** : `kube-prometheus-stack` 92.14.1 ([monitoring/values.yaml](monitoring/values.yaml)). Prometheus garde 7 jours de métriques. Grafana a son mot de passe admin dans le Secret `grafana-admin`. Alertmanager est désactivé. Les tableaux de bord versionnés sont dans [monitoring/dashboards/](monitoring/dashboards/).
- **`loki`** : Loki 7.5.0 en mode `SingleBinary` ([loki/values.yaml](loki/values.yaml)), sur un volume de 5 Gi, avec 7 jours de rétention.
- **`alloy`** : Alloy 1.14.4 ([alloy/values.yaml](alloy/values.yaml)), un seul pod qui lit les logs de tous les pods par l'API Kubernetes et les envoie à Loki.

Prometheus collecte tous les `ServiceMonitor` et `PodMonitor` du cluster :

| Composant | Moniteur | Défini dans |
| --- | --- | --- |
| Nessie | `ServiceMonitor` sur `/q/metrics` (port de gestion) | chart Nessie, créé dès que la CRD existe |
| Trino | `ServiceMonitor` sur `/metrics`, utilisateur `prometheus` sans mot de passe | [trino/values.yaml](trino/values.yaml) |
| Kafka | `PodMonitor` sur les brokers (`strimziMetricsReporter`) et Kafka Exporter (retard des groupes) | [kafka/kafka.yaml](kafka/kafka.yaml), [monitoring/values.yaml](monitoring/values.yaml) |
| Airflow | `ServiceMonitor` sur l'exporter StatsD du chart | [airflow/values.yaml](airflow/values.yaml), [monitoring/values.yaml](monitoring/values.yaml) |
| Kubernetes | nœuds, pods, conteneurs (node-exporter, kube-state-metrics, kubelet) | `kube-prometheus-stack` |

Accès à Grafana, avec le tableau de bord « WattLake » :

```bash
kubectl --context k3d-wattlake -n monitoring port-forward svc/monitoring-grafana 3000:80   # http://127.0.0.1:3000
kubectl --context k3d-wattlake -n monitoring get secret grafana-admin \
  -o jsonpath='{.data.admin-password}' | base64 --decode
```

Un tableau de bord se modifie dans le dépôt : `wattlake.json`, puis commit. Grafana n'a pas de volume, et une modification faite dans son interface est perdue au redémarrage.

## Dépannage

### Plus de résolution des noms externes après un changement de réseau

**Symptômes** : après un redémarrage de l'ordinateur ou un changement de réseau (Wi-Fi, partage de connexion), les Applications Argo CD passent en `Unknown` avec `ComparisonError: ... failed to get git client for repo`, et les logs de `argocd-repo-server` montrent `lookup github.com on 10.43.0.10:53: server misbehaving`. Les noms internes au cluster (`nessie`, `trino`...) résolvent toujours : seuls les noms externes échouent, dont l'API ODRÉ du DAG bronze.

**Cause** : CoreDNS transmet les noms externes au résolveur du nœud k3d sur lequel il tourne, qui est le DNS intégré de Docker (`192.168.64.1`). Docker y inscrit les serveurs DNS de l'ordinateur au démarrage du conteneur, lus dans `/run/systemd/resolve/resolv.conf`, et ne les met plus à jour ensuite. Deux cas en découlent :

- après un changement de réseau, le nœud interroge encore les serveurs de l'ancien réseau, qui ne répondent plus ;
- un nœud démarré pendant que l'ordinateur changeait de réseau n'a reçu aucun serveur : son `/etc/resolv.conf` contient `# NO EXTERNAL NAMESERVERS DEFINED`.

Chaque nœud démarre séparément : l'un peut être sain et l'autre non. Si le nœud défaillant est celui de CoreDNS, tout le cluster perd les noms externes.

**Vérifier** : `make cluster-dns-check`, en lecture seule. Pour chaque nœud (`server-0`, `agent-0`), la cible lit les serveurs DNS reçus de Docker (commentaire `ExtServers` de son `/etc/resolv.conf`) et les compare à ceux de l'ordinateur. Elle résout ensuite `github.com` et `odre.opendatasoft.com` par le résolveur du nœud, puis par CoreDNS. Elle échoue si un nœud n'a aucun serveur ou si un nom ne résout pas, et elle affiche les nœuds à redémarrer.

**Corriger** : d'abord, attendre que le réseau de l'ordinateur soit stable : `/run/systemd/resolve/resolv.conf` doit lister au moins un `nameserver`. Sinon, les nœuds redémarrés n'auront aucun serveur DNS.

- Un seul nœud en échec : `docker restart k3d-wattlake-agent-0` (le nom affiché par la cible), environ 1 minute. Seuls les pods de ce nœud redémarrent.
- Plusieurs nœuds, ou en cas de doute : `make cluster-restart`, environ 2 à 3 minutes.

Au démarrage des conteneurs, Docker relit les serveurs DNS de l'ordinateur. Aucune donnée n'est perdue : volumes conservés, catalogue Nessie dans PostgreSQL. Argo CD resynchronise ensuite tout seul.

Deux corrections permanentes ont été écartées :

- des serveurs DNS publics fixes dans CoreDNS (`1.1.1.1`, par exemple) : le cluster ne dépendrait plus du réseau local, mais les réseaux d'entreprise et les VPN bloquent souvent le DNS sortant, et toutes les requêtes partiraient chez un tiers ;
- faire écouter systemd-resolved sur l'interface Docker, qui suit les changements de réseau : c'est une configuration système de l'ordinateur, hors du dépôt, donc non reproductible.

