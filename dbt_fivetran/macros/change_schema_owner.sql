{% macro change_schema_owner() %}

  {%- set schemas = get_sisense_schemas() -%}

  {% for schema in schemas %}
    {% set sql %}
    ALTER SCHEMA {{ schema }} OWNER TO dbt_cloud;
    {% endset %}

    {{ log('Schema: ' ~ schema, info=True) }}
    {% do run_query(sql) %}
  {% endfor %}
  
{% endmacro %}