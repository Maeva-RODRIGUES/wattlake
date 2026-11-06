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

Le catalogue `wattlake` est configuré par les variables d'environnement `PYICEBERG_CATALOG__WATTLAKE__*`, définies dans `infra/airflow/values.yaml`.

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

La cible crée `ingestion/.venv` avec [uv](https://docs.astral.sh/uv/), en Python 3.13.13 et avec les contraintes d'Airflow 3.2.2, comme l'image Airflow, puis lance pytest. Les appels HTTP sont simulés avec respx, et les tests bronze utilisent un catalogue Iceberg local (SQLite et fichiers temporaires) : aucun test ne contacte l'API ni le cluster.
