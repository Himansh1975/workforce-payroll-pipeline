{#
  The few places where DuckDB and BigQuery SQL differ. Everything else in the
  models is standard SQL and runs unchanged on both.
#}

{# Monday-based week start, as a DATE. #}
{% macro week_start(col) %}
  {%- if target.type == 'bigquery' -%}
    date_trunc({{ col }}, week(monday))
  {%- else -%}
    cast(date_trunc('week', {{ col }}) as date)
  {%- endif -%}
{% endmacro %}

{# Money type with two decimals. #}
{% macro money_type() %}
  {%- if target.type == 'bigquery' -%} numeric {%- else -%} decimal(10, 2) {%- endif -%}
{% endmacro %}

{# Physical layout hints. BigQuery partitions and clusters; DuckDB ignores them. #}
{% macro bq_layout(partition_field=none, cluster_by=none) %}
  {%- if target.type == 'bigquery' -%}
    {%- set cfg = {} -%}
    {%- if partition_field -%}{%- do cfg.update({'partition_by': {'field': partition_field, 'data_type': 'date'}}) -%}{%- endif -%}
    {%- if cluster_by -%}{%- do cfg.update({'cluster_by': cluster_by}) -%}{%- endif -%}
    {{ config(**cfg) }}
  {%- endif -%}
{% endmacro %}
