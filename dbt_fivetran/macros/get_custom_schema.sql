{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}
    {%- if target.name == 'prod' and custom_schema_name is not none -%}

        {{ custom_schema_name | trim }}
    
    {%- elif target.name == 'default' and custom_schema_name is not none -%}

        {{ target.user }}__{{ custom_schema_name | trim }}

    {%- elif target.name == 'ci' and custom_schema_name is not none -%}

        {{ default_schema }}_{{ custom_schema_name | trim }}

    {%- else -%}

        {{ default_schema }}

    {%- endif -%}

{%- endmacro %}
