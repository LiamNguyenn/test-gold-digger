{% macro get_columns_to_mask_for_all_sources(database) %}
    {% set columns_to_mask = [] %}
    {% set table_catalogs = [] %}
    {% set table_schemas = [] %}
    {% set table_names = [] %}

    {% for ssource in graph.sources.values() %}
        {% do table_catalogs.append(ssource["database"]) %}
        {% do table_schemas.append(ssource["schema"]) %}
        {% do table_names.append(ssource["identifier"]) %}
        {% do columns_to_mask.extend(
            get_columns_to_mask(
                "source",
                ssource["identifier"],
                default_policy="no_mask",
            )
        ) %}
    {% endfor %}

    {# 
    Query columns metadata.
    #}
    {% set result = run_query(
        get_column_metadata_for_table_query(
            table_catalogs, table_schemas, table_names
        )
    ) %}

    {% set existing_columns = [] %}
    {% set out_columns_to_mask = [] %}

    {% if columns_to_mask %}
        {% for col in columns_to_mask %}
            {% set tuple = (
                col["database"],
                col["schema"],
                col["table"],
                col["column_name"],
                col["masking_role"],
            ) %}
            {% if tuple in existing_columns %}
                {% do exceptions.warn("Existing columns: " ~ tuple) %}
                {% continue %}
            {% endif %}

            {% for row in result.rows if row['column_name'] == col['column_name'] and row['table_catalog'] == col['database'] and row['table_schema'] == col['schema'] and row['table_name'] == col['table'] %}

                {% do col.update({"table_type": row["table_type"]}) %}
                {% do col.update({"data_type": row["data_type"]}) %}
                {% do col.update(
                    {
                        "character_maximum_length": row[
                            "character_maximum_length"
                        ]
                    }
                ) %}

                {{ out_columns_to_mask.append(col) }}
            {% endfor %}

            {% do existing_columns.append(tuple) %}
        {% endfor %}
    {% endif %}

    {{ return(out_columns_to_mask) }}
{% endmacro %}
