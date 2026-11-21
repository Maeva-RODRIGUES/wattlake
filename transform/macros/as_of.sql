{#
  The instant a run treats as "now", as a Trino timestamp with time zone.

  The dbt_transformations DAG sets the variable as_of to the run's logical time
  (run_after, ISO 8601 with offset): running the same run again rebuilds the
  same tables, whatever the clock says. Without the variable (a run started by
  hand, make dbt), it falls back to the current time.

  The value is checked before it reaches the SQL: an ISO 8601 date and time
  with an explicit offset, nothing else.
#}
{% macro as_of() -%}
  {%- set value = var('as_of', none) -%}
  {%- if value is none -%}
    current_timestamp
  {%- else -%}
    {%- set iso = '^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})$' -%}
    {%- if not modules.re.match(iso, value | string) -%}
      {{ exceptions.raise_compiler_error("as_of must be an ISO 8601 timestamp with an offset, got: " ~ value) }}
    {%- endif -%}
    from_iso8601_timestamp('{{ value }}')
  {%- endif -%}
{%- endmacro %}
