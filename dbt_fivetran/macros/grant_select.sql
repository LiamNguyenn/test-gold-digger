{% macro grant_select(schema, user) %}

  {% set sql %}
 
  GRANT USAGE ON SCHEMA {{ schema }} TO {{ user }};
  GRANT SELECT ON ALL TABLES IN SCHEMA {{ schema }} TO {{ user }};
  ALTER DEFAULT PRIVILEGES IN SCHEMA {{ schema }} GRANT SELECT ON TABLES TO {{ user }};

  {% endset %}

  {%- if target.name == 'prod' -%}
  {{ log('Granting select on all tables and views in schema ' ~ schema ~ ' to user ' ~ user, info=True) }}
  {% do run_query(sql) %}
  {% endif %}

{% endmacro %}
