# WattLake

Plateforme Lakehouse open source construite sur les données publiques du mix électrique français (RTE, éCO2mix).

> **Projet en cours**, démarré en novembre 2026. La feuille de route ci-dessous indique ce qui fonctionne déjà et ce qui reste à construire.

## Pourquoi ce projet

WattLake est un projet d'apprentissage : je l'ai lancé pour pratiquer, de bout en bout, une stack Lakehouse moderne (Iceberg, Nessie, Trino, Airflow, Kafka) déployée sur Kubernetes selon des pratiques DataOps.

J'ai choisi les données de production et de consommation électrique parce qu'elles réunissent deux usages classiques : un historique à charger en batch, et des mises à jour fréquentes qui se prêtent à un flux temps réel. Elles permettent aussi de répondre à des questions concrètes, comme l'évolution de la part des énergies renouvelables dans le mix.

## Aperçu

![Historique des commits de la branche main dans l'interface de Nessie](docs/images/nessie-commits.png)

*Historique de la branche `main` dans Nessie après un smoke test. Chaque opération lancée depuis Trino (suppression, création du namespace, création de la table, insertion) devient un commit, comme dans Git. C'est ce versionnement qui permettra de revenir à un état antérieur et de travailler sur des branches isolées avant de publier.*

![Détail de la requête de contrôle du smoke test dans l'interface de Trino](docs/images/trino-query.png)

*Requête de contrôle du smoke test dans l'interface de Trino : elle relit la table Iceberg et appelle `fail()` si le contenu n'est pas celui attendu. Ses deux étapes terminées montrent que Trino lit bien les données via le catalogue Nessie et le stockage SeaweedFS.*

Ces captures sont générées par `make screenshots`.

## Architecture

```mermaid
flowchart LR
    API[API publique RTE<br/>éCO2mix] -->|export Parquet, mois par mois| AF[Airflow<br/>DAG bronze]
    AF -->|PyIceberg,<br/>catalogue REST| BR[(bronze)]
    BR -.->|Asset : déclenche| DBT[Airflow<br/>DAG dbt]
    DBT -->|dbt build sur Trino,<br/>API native Nessie| SG[(silver, gold<br/>+ 44 tests)]
    NES[Nessie<br/>catalogue versionné] --- BR
    NES --- SG
    S3[SeaweedFS<br/>stockage S3] --- BR
    S3 --- SG
    API -.->|temps réel, étape 5| K[Kafka]
```

Toute la stack tourne sur un cluster Kubernetes local (k3d), déployée avec Helm et, à terme, synchronisée depuis ce dépôt par Argo CD. Les tables sont au format Iceberg. Le DAG bronze charge l'historique par PyIceberg, puis chaque chargement déclenche le DAG dbt, qui reconstruit silver et gold sur Trino et lance les tests. Kafka, en pointillés, est prévu pour le temps réel.

| Brique | Outil | Rôle |
| --- | --- | --- |
| Stockage objet | SeaweedFS (API S3) | Stocke les fichiers Parquet des tables |
| Format de table | Apache Iceberg | Organise les fichiers en tables (schéma, partitions, snapshots) |
| Catalogue | Nessie | Versionne l'état des tables, avec des branches comme Git |
| Moteur de requêtes | Trino | Exécute le SQL sur les tables Iceberg |
| Ingestion | Python, PyIceberg | Télécharge les exports de l'API et les écrit dans bronze, un commit par mois |
| Transformations | dbt (adaptateur Trino) | Construit les couches silver et gold, avec des tests |
| Orchestration | Airflow | Planifie l'ingestion batch et lance dbt chaque fois que bronze change |
| Streaming | Kafka (à venir) | Transportera les mises à jour en temps réel |
| Déploiement | Kubernetes, Helm, puis GitHub Actions (ADR 005) et Argo CD, à venir | Rend la stack reproductible, puis testée et déployée en GitOps |

## Feuille de route

**Étape 1 : le socle**
- [x] Création du dépôt et de la documentation
- [x] Cluster k3d et déploiement de SeaweedFS, Nessie et Trino via Helm
- [x] Première table Iceberg créée depuis Trino et visible dans Nessie
- [x] DAG Airflow chargeant l'historique dans la couche bronze

**Étape 2 : les transformations**
- [x] Couches silver et gold avec dbt-trino
- [x] Tests de qualité des données dans dbt
- [x] dbt lancé par Airflow chaque fois que la couche bronze change

**Étape 3 : DataOps**
- [ ] Intégration continue (choix documenté dans l'ADR 005) : lint, tests Python, tests dbt, construction des images
- [ ] Déploiement GitOps avec Argo CD
- [ ] Write-Audit-Publish : écriture sur une branche Nessie, contrôles qualité, puis fusion dans `main`
- [ ] Identifiants déplacés dans des Secrets Kubernetes

**Étape 4 : temps réel et observabilité**
- [ ] Flux temps réel : API, Kafka, Iceberg
- [ ] Métriques et logs des traitements

## Démarrer

### Prérequis

| Outil | Version testée | Rôle |
| --- | --- | --- |
| Docker | 28.1 | Fait tourner les nœuds du cluster k3d et construit l'image Airflow |
| k3d | 5.9 (k3s 1.35.5) | Crée le cluster Kubernetes local |
| kubectl | 1.35 | Pilote le cluster |
| Helm | 3.12 | Déploie les composants de la stack |
| Python | 3.12 | Utilisé par le smoke test |
| uv | 0.11 | Crée les environnements Python 3.13 du code d'ingestion et du projet dbt |
| direnv | 2.32 | Charge les variables d'environnement du projet (optionnel) |

Prévoir 16 Go de mémoire libre : les limites mémoire des composants atteignent environ 13 Go, dont 5 Go pour Trino et 5,5 Go pour Airflow, et les Jobs d'installation d'Airflow ajoutent temporairement 2 Go.

### Isoler l'environnement du projet

Le projet utilise son propre fichier kubeconfig et sa propre configuration Helm, pour ne jamais toucher à un autre cluster. Le Makefile vérifie ces variables et refuse de s'exécuter si elles ne sont pas définies.

```bash
cp .envrc.example .envrc
direnv allow
```

Sans direnv, exporter à la main les quatre variables de [`.envrc.example`](.envrc.example) dans le terminal.

### Lancer la stack

```bash
make up                 # cluster k3d, puis SeaweedFS, PostgreSQL, Nessie, Trino et Airflow (image construite et importée dans k3d)
make smoke-test         # test de bout en bout
make rest-catalog-test  # écriture PyIceberg par le catalogue REST, lue depuis Trino
make dbt ARGS="debug"   # projet dbt : configuration et connexion à Trino (voir transform/README.md)
```

`make up` peut être relancé : il ne recrée pas le cluster existant et met à jour les composants. Pour un seul composant, utiliser sa cible : `make cluster-up`, `make seaweedfs-up`, `make postgres-up`, `make nessie-up`, `make trino-up` ou `make airflow-up`.

`make rest-catalog-test` vérifie le chemin d'écriture de l'ingestion (voir l'[ADR 004](docs/adr/004-catalogue-rest-ingestion.md)) : depuis un pod éphémère, PyIceberg crée une table par le catalogue REST de Nessie, y ajoute des lignes puis en remplace une partie. Trino relit ensuite la table par l'API native, et Nessie doit montrer un commit par écriture.

Le smoke test crée une table Iceberg depuis Trino, la relit, vérifie que ses fichiers sont dans SeaweedFS et que son commit apparaît dans Nessie. Il se termine par :

```
==> Smoke test passed
```

### Charger les données et lancer les transformations

Sur un cluster neuf, les deux DAG d'Airflow sont en pause, comme tout nouveau DAG. Les activer lance la chaîne complète :

```bash
kubectl --context k3d-wattlake -n wattlake exec airflow-scheduler-0 -c scheduler -- \
  airflow dags unpause dbt_transformations
kubectl --context k3d-wattlake -n wattlake exec airflow-scheduler-0 -c scheduler -- \
  airflow dags unpause eco2mix_history_to_bronze
```

Le DAG `eco2mix_history_to_bronze` rattrape alors l'historique mois par mois depuis janvier 2012, en quelques minutes. Chaque mois chargé met à jour l'Asset de la table bronze, ce qui déclenche `dbt_transformations` : silver et gold sont reconstruites et les tests lancés. Les détails sont dans [ingestion/README.md](ingestion/README.md) et [transform/README.md](transform/README.md).

### Interroger la stack

```bash
# CLI Trino, depuis le pod coordinator
kubectl --context k3d-wattlake -n wattlake exec -it deploy/trino-coordinator -- trino

# Interfaces web, chacune dans son terminal
kubectl --context k3d-wattlake -n wattlake port-forward svc/trino 8080:8080     # Trino : http://127.0.0.1:8080
kubectl --context k3d-wattlake -n wattlake port-forward svc/nessie 19120:19120  # Nessie : http://127.0.0.1:19120
kubectl --context k3d-wattlake -n wattlake port-forward svc/airflow-api-server 8081:8080  # Airflow : http://127.0.0.1:8081 (admin / admin-dev)
```

Une fois la chaîne passée, la part des renouvelables se lit dans la couche gold :

```sql
SELECT year(month) AS annee,
       round(sum(renewable_strict_mwh) / sum(production_with_detail_mwh), 3) AS part_renouvelable
FROM iceberg.gold.renewable_share_monthly
GROUP BY 1 ORDER BY 1;
```

### Régénérer les captures

```bash
make screenshots     # smoke test, puis captures de Nessie et Trino dans docs/images/
```

Au premier lancement, la cible crée un environnement virtuel `.venv` dans le projet et y installe Playwright et un Chromium headless (environ 450 Mo). Rien n'est installé en dehors du projet : supprimer `.venv` suffit à tout retirer.

### Mettre en pause et supprimer

```bash
make cluster-stop    # arrête les conteneurs du cluster, les données sont conservées
make cluster-start   # redémarre le cluster
make down            # supprime le cluster et toutes ses données (alias de cluster-down)
```

Nessie conserve pour l'instant son historique en mémoire : tout redémarrage de son pod efface les commits, alors que les fichiers restent dans SeaweedFS. Un stockage persistant est prévu dans la feuille de route.

## Choix techniques

Chaque décision importante est documentée dans un ADR (Architecture Decision Record), dans le dossier [`docs/adr`](docs/adr) :

- [ADR 001 : choix du stockage objet](docs/adr/001-stockage-objet.md), ou pourquoi SeaweedFS plutôt que MinIO.
- [ADR 002 : API utilisée pour accéder au catalogue Nessie](docs/adr/002-api-catalogue-nessie.md), ou pourquoi l'API native de Nessie plutôt que son catalogue REST Iceberg.
- [ADR 003 : base de métadonnées d'Airflow](docs/adr/003-base-metadonnees-airflow.md), ou pourquoi une image PostgreSQL officielle plutôt que celle embarquée par le chart.
- [ADR 004 : catalogue REST Iceberg pour l'ingestion](docs/adr/004-catalogue-rest-ingestion.md), qui remplace l'ADR 002 : pourquoi PyIceberg écrit par le catalogue REST de Nessie avec ses propres clés S3, pendant que Trino reste sur l'API native.
- [ADR 005 : plateforme d'intégration continue](docs/adr/005-integration-continue.md), ou pourquoi GitHub Actions plutôt que GitLab CI, dont la CI pour un dépôt GitHub est réservée aux offres payantes.

## Ce que je ferais en production

Ce projet tourne en local, avec des choix simplifiés. Sur une plateforme en production, je prévoirais en plus :

- un stockage persistant pour Nessie (PostgreSQL), au lieu du stockage en mémoire utilisé pour démarrer ;
- une gestion des secrets centralisée, par exemple avec un coffre-fort de secrets ;
- de la haute disponibilité sur le stockage objet et sur le coordinateur Trino ;
- de la supervision et des alertes sur la fraîcheur et la qualité des données.

## Auteure

Maëva Rodrigues, Data Engineer.
