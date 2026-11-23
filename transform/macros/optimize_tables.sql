{#-
    Compact the files of the Iceberg tables (docs/adr/013-maintenance-iceberg.md,
    docs/adr/017-reconstruction-incrementale.md). Trino's optimize rewrites the
    files smaller than the threshold into fewer, larger ones, and folds in the
    delete files. The content of a table does not change, only its layout: a
    new snapshot, no new rows. Old files stay in the storage until Nessie GC
    removes them.

    The tables are read from the target's catalog: --target dev compacts main.
-#}

{% macro optimize_relation(relation, file_size_threshold) %}
    {% do log('Compacting ' ~ relation ~ ' (files under ' ~ file_size_threshold ~ ')', info=True) %}
    {% do run_query(
        'alter table ' ~ relation ~ " execute optimize(file_size_threshold => '" ~ file_size_threshold ~ "')"
    ) %}
{% endmacro %}

{#-
    Bronze: the history is appended month by month and the real-time table
    every few minutes (about 100 files of 30 KB a day). Run by the Airflow DAG
    iceberg_maintenance, every day, on main, where the writer appends:
        dbt run-operation optimize_bronze_tables --target dev
-#}
{% macro optimize_bronze_tables(file_size_threshold='64MB') %}
    {% for table in ['eco2mix_national_history', 'eco2mix_national_realtime'] %}
        {% do optimize_relation(source('bronze', table), file_size_threshold) %}
    {% endfor %}
{% endmacro %}

{#-
    Silver and daily gold: built incrementally, every hourly merge adds small
    files and delete files. Run by the Airflow DAG dbt_transformations, once a
    day, after it publishes (the same DAG run: no dbt build in progress, so
    the compaction commit cannot collide with a publication):
        dbt run-operation optimize_silver_gold_tables --target dev
    The monthly gold table is rebuilt in full and needs none.
-#}
{% macro optimize_silver_gold_tables(file_size_threshold='64MB') %}
    {% for model in ['eco2mix_national', 'energy_mix_daily'] %}
        {% do optimize_relation(ref(model), file_size_threshold) %}
    {% endfor %}
{% endmacro %}
