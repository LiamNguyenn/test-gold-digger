{% macro generate_like_statements(column_name, patterns) %}
  {%- set like_statements = [] -%}
  {%- for pattern in patterns -%}
    {%- do like_statements.append(column_name ~ " like '%" ~ pattern ~ "%'") -%}
  {%- endfor -%}
  {{- like_statements|join("\nor ") -}}
{% endmacro %}
