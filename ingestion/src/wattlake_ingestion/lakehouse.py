"""Names of the lakehouse tables, shared by the loaders and the Airflow DAGs.

No heavy import here: DAG files import this module every time the
dag-processor parses them.
"""

BRONZE_NAMESPACE = "bronze"
BRONZE_TABLE = f"{BRONZE_NAMESPACE}.eco2mix_national_history"
# Identifies the bronze table as an Airflow Asset: the bronze DAG updates it,
# the dbt DAG is scheduled on it.
BRONZE_TABLE_URI = "iceberg://wattlake/bronze/eco2mix_national_history"
