# dags

DAG Airflow de WattLake. Ce dossier est copié dans l'image Airflow personnalisée (`ingestion/Dockerfile`), dans `/opt/airflow/dags`, le dossier lu par le dag-processor.

- `eco2mix_history_to_bronze` : charge l'historique dans la couche bronze, mois par mois.
- `eco2mix_history_refresh` : recharge chaque semaine les 24 derniers mois de l'historique, en n'écrivant que les mois que RTE a publiés ou révisés depuis.
- `dbt_transformations` : construit les couches silver et gold avec dbt, toutes les heures et chaque fois que la table bronze de l'historique change (Asset). Silver et le gold quotidien sont mis à jour en incrémental (ADR 017) : le run de 11:00 UTC les compacte après la publication, et celui du dimanche à 11:00 UTC les reconstruit en entier (`--full-refresh`). Les deux règles sont dans [wattlake_ingestion/transform_schedule.py](../src/wattlake_ingestion/transform_schedule.py), testées sans Airflow.
- `iceberg_maintenance` : compacte chaque jour les fichiers des deux tables bronze (`optimize` de Trino, par une macro dbt), puis lance Nessie GC dans un Job Kubernetes.

Les DAG importent le paquet `wattlake_ingestion`, et le DAG dbt appelle l'environnement dbt de l'image. Une modification des DAG, du paquet ou du projet dbt (`transform/`, copié dans l'image) arrive dans Airflow en deux temps : la CI construit et publie l'image du commit sur `main`, puis son SHA est reporté dans `images.airflow.tag` de `infra/airflow/values.yaml`, qu'Argo CD déploie.

Airflow ne lit que les fichiers Python : ce README est ignoré.
