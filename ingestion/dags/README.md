# dags

DAG Airflow de WattLake. Ce dossier est copié dans l'image Airflow personnalisée (`ingestion/Dockerfile`), dans `/opt/airflow/dags`, le dossier lu par le dag-processor.

Les DAG importent le paquet `wattlake_ingestion`, installé dans la même image. Pour qu'une modification arrive dans Airflow, il faut reconstruire l'image, incrémenter `AIRFLOW_IMAGE_TAG` dans le `Makefile`, puis lancer `make airflow-up`.

Airflow ne lit que les fichiers Python : ce README est ignoré.
