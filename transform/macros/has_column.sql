{#
  True when the model's existing table ({{ this }}) has the given column.

  Used by an incremental model that adds a column its incremental logic then
  reads: on the first run against an older table, the column is not there yet,
  and the model recomputes everything instead of failing; on_schema_change
  then adds the column. Unit tests override it (overrides.macros).
#}
{% macro this_has_column(column_name) -%}
  {%- set columns = adapter.get_columns_in_relation(this) | map(attribute='name') | map('lower') | list -%}
  {{- return(column_name | lower in columns) -}}
{%- endmacro %}
