{%- macro get_columns_to_mask_for_model(
    resource_type,
    database,
    schema,
    table_name=none,
    default_policy=None
) -%}
    {% set columns_to_mask = get_columns_to_mask(
        resource_type, table_name, default_policy
    ) %}

    {% set result = run_query(
        get_column_metadata_for_table_query(
            [database], [schema], [table_name]
        )
    ) %}
    {% for col in columns_to_mask %}
        {% for row in result.rows if row['column_name'] == col['column_name'] %}
            {% do col.update({"table_type": row["table_type"]}) %}
            {% do col.update({"data_type": row["data_type"]}) %}
            {% do col.update(
                {
                    "character_maximum_length": row[
                        "character_maximum_length"
                    ]
                }
            ) %}
        {% endfor %}
    {% endfor %}

    {{ return(columns_to_mask) }}
{% endmacro %}
