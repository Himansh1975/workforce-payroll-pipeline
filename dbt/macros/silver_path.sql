{# Where the Spark jobs write silver parquet. Relative to the dbt folder by default. #}
{% macro silver_path() %}{{ env_var('SILVER_PATH', '../data/silver') }}{% endmacro %}
