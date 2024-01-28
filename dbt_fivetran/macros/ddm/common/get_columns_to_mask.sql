{%- macro get_columns_to_mask(
    resource_type, table=none, default_policy=None
) -%}

{# 
Get a list of columns that have a masking policy defined in the meta section of the column definition.
#}
    {% if not (resource_type == "source" or resource_type == "model") %}
        {% do exceptions.raise_compiler_error(
            '"resource_type" must be "source" or "model"'
        ) %}
    {% endif %}

    {% if resource_type == "source" %}
        {% set search_path = graph.sources.values() %}
        {% set table_key = "identifier" %}
    {% elif resource_type == "model" %}
        {% set search_path = graph.nodes.values() %}
        {% set table_key = "alias" %}
    {% endif %}

    {% if table %}
        {% set name_test = "equalto" %} {% set name_match = table.lower() %}
    {% else %}
        {% set name_test = "ne" %}  {# not equal to #}
        {% set name_match = none %}
    {% endif %}

    {% set column_policies = [] %}
    {% set column_info = dict() %}

    {%- if execute -%}

        {%- for node in search_path | selectattr(
            "resource_type", "equalto", resource_type
        ) | selectattr("name", name_test, name_match) -%}

            {# {% do log(node.name, info=true) %}  #}
            {%- for column in node.columns.values() | selectattr("meta") -%}
                {# {% do log(column.meta, info=true) %} #}
                {%- if column.meta["sensitive"] -%}

                    {% set column_info = {
                        "database": node.database,
                        "schema": node.schema,
                        "table": node[table_key],
                        "column_name": column.name,
                        "masking_rules": column.meta.get(
                            "masking_rules", None
                        ),
                        "masking_role": column.meta.get(
                            "policy", default_policy
                        ),
                    } %}
                    {% do column_policies.append(column_info) %}

                {%- endif -%}

            {%- endfor -%}

        {%- endfor -%}

        {{ return(column_policies) }}

    {%- endif -%}

{%- endmacro -%}
