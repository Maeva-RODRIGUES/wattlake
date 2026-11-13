{#-
    Compact the files of the bronze tables (docs/adr/013-maintenance-iceberg.md).
    Run by the Airflow DAG iceberg_maintenance, every night:
        dbt run-operation optimize_bronze_tables --target dev

    The history is appended month by month and the real-time table every few
    minutes (about 100 files of 30 KB a day): Trino's optimize rewrites the
    files smaller than the threshold into fewer, larger ones. The content of the
    tables does not change, only its layout: a new snapshot, no new rows.
    Silver and gold are rewritten in full by every dbt run and need no compaction.

    The tables are read from the target's catalog: --target dev compacts main,
    where the writer appends. Old files stay in the storage until Nessie GC
    removes them (step 8.2).
-#}
{% macro optimize_bronze_tables(file_size_threshold='64MB') %}
    {% for table in ['eco2mix_national_history', 'eco2mix_national_realtime'] %}
        {% set relation = source('bronze', table) %}
        {% do log('Compacting ' ~ relation ~ ' (files under ' ~ file_size_threshold ~ ')', info=True) %}
        {% do run_query(
            'alter table ' ~ relation ~ " execute optimize(file_size_threshold => '" ~ file_size_threshold ~ "')"
        ) %}
    {% endfor %}
{% endmacro %}
