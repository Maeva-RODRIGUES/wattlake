# dags

DAG Airflow de WattLake. Ce dossier est copié dans l'image Airflow personnalisée (`ingestion/Dockerfile`), dans `/opt/airflow/dags`, le dossier lu par le dag-processor.

- `eco2mix_history_to_bronze` : charge l'historique dans la couche bronze, mois par mois.
- `dbt_transformations` : construit les couches silver et gold avec dbt, chaque fois que la table bronze change (Asset).

Les DAG importent le paquet `wattlake_ingestion`, et le DAG dbt appelle l'environnement dbt de l'image. Pour qu'une modification arrive dans Airflow, il faut reconstruire l'image, incrémenter `AIRFLOW_IMAGE_TAG` dans le `Makefile`, puis lancer `make airflow-up`. C'est aussi le cas pour une modification du projet dbt (`transform/`), copié dans l'image.

Airflow ne lit que les fichiers Python : ce README est ignoré.
