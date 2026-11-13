# ingestion

Code Python d'extraction des données RTE et DAG Airflow qui chargent la couche bronze.

## Paquet `wattlake_ingestion`

Le module `wattlake_ingestion.odre` interroge l'API publique d'ODRÉ, qui publie les données éCO2mix de RTE (voir [docs/sources.md](../docs/sources.md)). Il ne dépend pas d'Airflow : les DAG l'importent, et il se teste seul.

```python
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from wattlake_ingestion.odre import HISTORY_DATASET, OdreClient

paris = ZoneInfo("Europe/Paris")
with OdreClient() as client:
    client.export_to_file(
        HISTORY_DATASET,
        datetime(2026, 8, 1, tzinfo=paris),
        datetime(2026, 8, 2, tzinfo=paris),
        Path("eco2mix-2026-08-01.parquet"),
    )
```

- **Export plutôt que pagination** : l'endpoint `/records` s'arrête à 10 000 lignes, l'historique en compte plus de 500 000. Le client utilise `/exports/parquet`, filtré sur `date_heure`.
- **Période `[début, fin)`** : bornes avec fuseau horaire obligatoire, converties en UTC, fin exclue. Deux périodes qui se suivent ne se chevauchent pas.
- **Nouvelles tentatives** : sur erreur réseau, réponse 429 ou 5xx, avec un délai qui double à chaque essai, ou celui de l'en-tête `Retry-After`. Une autre erreur 4xx signale une requête fausse et n'est pas relancée.
- **Pas de fichier partiel** : le téléchargement s'écrit dans un fichier `.part`, renommé à la fin, et supprimé en cas d'échec.

## Couche bronze

Le module `wattlake_ingestion.bronze` charge un export dans la table Iceberg `bronze.eco2mix_national_history`, via le catalogue REST de Nessie (voir l'[ADR 004](../docs/adr/004-catalogue-rest-ingestion.md)).

- **Schéma explicite** : les colonnes de la source avec leurs types d'origine, y compris les nombres publiés en texte, plus `_source_dataset` et `_ingested_at`. Une colonne manquante ou inattendue fait échouer le chargement.
- **Un commit par période** : `load_period` remplace les lignes de `[début, fin)` par un `overwrite` filtré. Recharger un mois donne le même résultat.
- **Un mois vide n'écrit rien** : une réponse vide de l'API ne peut pas effacer un mois déjà chargé.
- **Pas de partitionnement** : environ 500 000 lignes, et les statistiques min/max de chaque fichier suffisent à ignorer les autres mois. À réévaluer avec le temps réel de l'étape 5.

## DAG `eco2mix_history_to_bronze`

Le DAG ([dags/eco2mix_history_to_bronze.py](dags/eco2mix_history_to_bronze.py)) charge l'historique mois par mois, depuis janvier 2012, avec le rattrapage d'Airflow.

- **Mois en heure de Paris**, comme RTE les publie : janvier 2012 commence le 31 décembre 2011 à 23:00 UTC.
- **Une exécution à la fois** : deux mois écrits en même temps dans la table entreraient en conflit dans Nessie.
- **Une seule tâche par mois** : téléchargement de l'export, puis chargement dans bronze, avec deux nouvelles tentatives.
- **Asset de la table bronze** : chaque chargement qui écrit des lignes met à jour l'Asset `bronze.eco2mix_national_history`, ce qui déclenche le DAG `dbt_transformations`. Un mois vide termine la tâche à l'état « skipped » : aucun événement n'est émis, et dbt n'est pas relancé pour rien.

## DAG `dbt_transformations`

Le DAG ([dags/dbt_transformations.py](dags/dbt_transformations.py)) construit les couches silver et gold avec un `dbt build`, qui lance aussi les tests. Il a deux déclencheurs (`AssetOrTimeSchedule`), le premier arrivé l'emporte :

- **toutes les heures**, à l'heure pile (UTC), pour intégrer le temps réel que le writer ajoute à bronze toutes les 5 minutes, hors d'Airflow ([ADR 012](../docs/adr/012-temps-reel-silver-gold.md)). Gold a donc au plus une heure environ de retard sur le flux ;
- **l'Asset de la table bronze de l'historique**, mis à jour par chaque chargement. Pendant un rattrapage, les événements accumulés sont consommés par une seule exécution.

Chaque exécution réécrit silver et gold, environ 14 Mo de fichiers Parquet. Nessie gardant les versions précédentes, le stockage grossit d'environ 340 Mo par jour tant que son GC n'est pas en place (voir « Limites et suites possibles » dans le [README](../README.md)).

Il applique le Write-Audit-Publish sur une branche Nessie (voir l'[ADR 008](../docs/adr/008-write-audit-publish.md)) :

1. `create_audit_branch` recrée la branche `audit` depuis la tête de `main` ;
2. `dbt_build` lance `dbt build --target audit` : silver et gold sont écrits et testés sur la branche, par le catalogue Trino `iceberg_audit` ;
3. `publish` fusionne `audit` dans `main` en un commit, seulement si tous les tests passent.

Après un test en échec, `main` reste inchangé et la branche `audit` garde le build fautif, interrogeable dans Trino (`iceberg_audit.silver.*`, `iceberg_audit.gold.*`) jusqu'à l'exécution suivante. Le client Nessie, dans `wattlake_ingestion.nessie`, appelle l'API REST v2 avec `httpx`.

- **dbt isolé** : dbt tourne dans son propre environnement virtuel de l'image (`/opt/dbt/venv`), installé depuis `transform/requirements.txt`. Ses versions figées diffèrent des contraintes d'Airflow pour 26 paquets communs : les deux ne peuvent pas partager un environnement.
- **Projet dbt dans l'image** : le projet et ses paquets (`dbt deps`, exécuté à la construction) sont copiés dans `/opt/wattlake/transform`. Les fichiers produits par dbt (`target/`, `logs/`) vont dans `/tmp/dbt`.
- **Même profil qu'en local** : la tâche fixe `TRINO_HOST` et `TRINO_PORT` sur le service Trino du cluster.
- Le nom et l'URI de l'Asset sont définis dans `wattlake_ingestion.lakehouse`, module sans import lourd, que les deux DAG importent.

Le catalogue `wattlake` est configuré par les variables d'environnement `PYICEBERG_CATALOG__WATTLAKE__*`, définies dans `infra/airflow/values.yaml`. Les clés S3 sont celles de l'identité SeaweedFS `airflow`, lues dans le Secret `airflow-credentials` (voir l'[ADR 006](../docs/adr/006-gestion-des-secrets.md)).

## DAG `iceberg_maintenance`

Le DAG ([dags/iceberg_maintenance.py](dags/iceberg_maintenance.py)) compacte chaque nuit, à 02:30 UTC, les fichiers des deux tables bronze ([ADR 013](../docs/adr/013-maintenance-iceberg.md)). Le writer temps réel ajoute environ 100 fichiers de 30 Ko par jour : `ALTER TABLE … EXECUTE optimize` les réécrit en quelques gros fichiers, sans changer le contenu. Testé sur la branche `audit` : 176 fichiers ramenés à 1 pour l'historique, 43 à 1 pour le temps réel, avec les mêmes lignes et la même somme de contrôle.

- **Par une macro dbt** ([transform/macros/optimize_bronze_tables.sql](../transform/macros/optimize_bronze_tables.sql)), lancée par `dbt run-operation optimize_bronze_tables --target dev` : elle réutilise la connexion de dbt à Trino et les sources du projet.
- **Directement sur `main`**, sans Write-Audit-Publish : le contenu des tables ne change pas. Si le compactage et le writer commitent la même table au même moment, Nessie rejette l'un des deux : le compactage recommence (deux nouvelles tentatives), ou le writer redémarre et retrouve ses offsets dans les données.
- **Les anciens fichiers restent** dans SeaweedFS jusqu'au passage de Nessie GC.

### Recharger tout l'historique

Après une perte du catalogue Nessie (base `nessie` de PostgreSQL perdue, par exemple), les fichiers restent dans SeaweedFS mais les tables ne sont plus connues. Un simple redémarrage de Nessie ne perd rien : son catalogue est dans PostgreSQL (voir l'[ADR 007](../docs/adr/007-stockage-versions-nessie.md)). Le chargement étant idempotent, il suffit de relancer toutes les exécutions du DAG bronze :

```bash
S="kubectl --context k3d-wattlake -n wattlake exec airflow-scheduler-0 -c scheduler --"
$S airflow dags pause dbt_transformations
$S airflow tasks clear eco2mix_history_to_bronze --yes
# attendre la fin des 178 exécutions, puis :
$S airflow dags unpause dbt_transformations
$S airflow dags trigger dbt_transformations
```

Mettre `dbt_transformations` en pause pendant le rechargement évite des dizaines de reconstructions de silver et gold : chaque mois chargé émet un événement d'Asset, et sans pause dbt se relance dès qu'il est libre (63 exécutions constatées lors d'un rechargement, en ralentissant le chargement).

Le déclenchement manuel à la fin est indispensable. Airflow ne met en file les événements d'Asset que pour les DAG actifs : ceux émis pendant la pause sont perdus pour `dbt_transformations`, et le réactiver ne lance rien. Constaté lors d'un rechargement : 176 événements émis, aucune exécution de dbt après la réactivation. Une seule exécution manuelle reconstruit ensuite silver et gold depuis tout bronze.

### Tester un mois sans le scheduler

`airflow dags test` exécute le DAG dans le pod, pour la période qui se termine à la date passée en argument. Pour charger août 2026 :

```bash
kubectl --context k3d-wattlake -n wattlake exec airflow-scheduler-0 -c scheduler -- \
  airflow dags test eco2mix_history_to_bronze 2026-09-01T00:00:00+02:00
```

Attention : ce test enregistre une exécution manuelle dont la date logique est la **fin** du mois testé. C'est aussi la date logique, le **début** de sa période, de l'exécution planifiée du mois suivant. Tant que l'exécution manuelle existe, le scheduler ne crée plus aucune exécution planifiée et répète `run already exists; skipping dagrun creation` dans ses logs. Après le test, supprimer l'exécution manuelle, par exemple avec l'API REST d'Airflow : `DELETE /api/v2/dags/eco2mix_history_to_bronze/dagRuns/{run_id}`.

## Tests

```bash
make ingestion-test
```

La cible crée `ingestion/.venv` avec [uv](https://docs.astral.sh/uv/), en Python 3.13.13 et avec les mêmes contraintes que l'image Airflow, puis lance pytest. Les appels HTTP sont simulés avec respx, et les tests bronze utilisent un catalogue Iceberg local (SQLite et fichiers temporaires) : aucun test ne contacte l'API ni le cluster.

## Versions des dépendances

L'image et les tests installent le paquet avec deux fichiers de contraintes :

- les contraintes d'Airflow 3.2.2, qui fixent la version de la plupart des dépendances ;
- [constraints.txt](constraints.txt), qui fixe celles que les contraintes d'Airflow ne listent pas : les dépendances de PyIceberg (`strictyaml`, `pyroaring`), PyIceberg lui-même et les outils de test.

Sans ce second fichier, ces paquets prendraient la dernière version publiée au moment de la construction : deux images du même commit pourraient différer.

```bash
make ingestion-lock         # régénère constraints.txt, en ignorant les versions de moins de 14 jours
make ingestion-lock-check   # échoue si un paquet installé n'est fixé par aucun des deux fichiers (lancé en CI)
```

`constraints.txt` est généré par `uv pip compile` à partir de `pyproject.toml` et des contraintes d'Airflow, puis filtré par [tools/ingestion_constraints.py](../tools/ingestion_constraints.py) pour ne garder que les paquets absents des contraintes d'Airflow. Le contrôle compare les paquets installés aux deux fichiers, sans relancer la résolution : il échoue quand une nouvelle dépendance apparaît, pas quand une nouvelle version sort en amont. Après un changement de `pyproject.toml` ou une montée de version d'Airflow, relancer `make ingestion-lock` et relire le résultat.
