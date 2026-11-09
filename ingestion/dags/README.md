# dags

DAG Airflow de WattLake. Ce dossier est copié dans l'image Airflow personnalisée (`ingestion/Dockerfile`), dans `/opt/airflow/dags`, le dossier lu par le dag-processor.

- `eco2mix_history_to_bronze` : charge l'historique dans la couche bronze, mois par mois.
- `dbt_transformations` : construit les couches silver et gold avec dbt, chaque fois que la table bronze change (Asset).

Les DAG importent le paquet `wattlake_ingestion`, et le DAG dbt appelle l'environnement dbt de l'image. Une modification des DAG, du paquet ou du projet dbt (`transform/`, copié dans l'image) arrive dans Airflow en deux temps : la CI construit et publie l'image du commit sur `main`, puis son SHA est reporté dans `images.airflow.tag` de `infra/airflow/values.yaml`, qu'Argo CD déploie.

Airflow ne lit que les fichiers Python : ce README est ignoré.
