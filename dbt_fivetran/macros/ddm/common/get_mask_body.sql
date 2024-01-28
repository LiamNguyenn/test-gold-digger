{%- macro get_mask_body(column_name, data_type) -%}

    {# Default maksing for each data type #}
    {% set data_type = data_type | lower %}
    {% if data_type == "character varying" or data_type == "text" %}
        {% set mask = "md5({})".format(column_name) %}
    {% elif data_type == "timestamp with time zone" or data_type == "timestamp without time zone" %}
        {% set mask = "NULL" %}
    {% elif data_type == "supper" %} {% set mask = "NULL" %}
    {% elif data_type == "variant" %} {% set mask = "'{***MASKED***}'" %}
    {% elif data_type == "date" %} {% set mask = "NULL" %}
    {% elif data_type == "double precision" %} {% set mask = "0.0" %}
    {% elif data_type == "number" %} {% set mask = "0" %}
    {% elif data_type == "boolean" %} {% set mask = "NULL" %}
    {% else %} {% set mask = "NULL" %}
    {% endif %}

    {% set body %}
  case 
    when current_user = 'dbt_cloud'::varchar or current_user = 'masteruser'::varchar then {{ column_name }} -- Skip if current user is admin user.
    else {{ mask }} 
  end 
    {% endset %}

    {{ return(body) }}

{% endmacro %}
