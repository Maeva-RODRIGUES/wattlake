{#-
    Use a model's custom schema as is (gold), instead of dbt's default
    "<target schema>_<custom schema>" (silver_gold). Models without a custom
    schema go to the target schema of the profile (silver).
-#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
