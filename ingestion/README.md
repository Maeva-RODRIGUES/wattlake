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

## Tests

```bash
make ingestion-test
```

La cible crée `ingestion/.venv` avec [uv](https://docs.astral.sh/uv/), en Python 3.13.13, la version de l'image Airflow, puis lance pytest. Les appels HTTP sont simulés avec respx : les tests ne contactent pas l'API.
