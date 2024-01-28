{% macro grant_select_sisense() %}

  {%- set schemas = get_sisense_schemas() -%}

  {% for schema in schemas %}
    {{ log('Schema: ' ~ schema, info=True) }}
    {% do grant_select(schema, 'sisense') %}
  {% endfor %}
  
{% endmacro %}