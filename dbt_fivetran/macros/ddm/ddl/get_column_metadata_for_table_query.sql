{% macro get_column_metadata_for_table_query(
    table_catalogs, table_schemas, table_names
) %}
    select
        t.table_catalog,
        t.table_schema,
        t.table_name,
        case
            t.table_type when 'BASE TABLE' then 'TABLE' else t.table_type
        end as table_type,
        c.column_name,
        c.data_type,
        c.character_maximum_length
    from svv_tables t
    inner join
        svv_columns c
        on c.table_catalog = t.table_catalog
        and c.table_schema = t.table_schema
        and c.table_name = t.table_name
    where
        t.table_catalog in ('{{ table_catalogs | unique | join("', '") }}')
        and t.table_schema in ('{{ table_schemas | unique | join("', '") }}')
        and t.table_name in ('{{ table_names | unique | join("', '") }}')
        and t.table_type in ('BASE TABLE', 'VIEW')
    ;
{% endmacro %}
