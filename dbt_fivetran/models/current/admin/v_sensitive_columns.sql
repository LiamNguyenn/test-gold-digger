-- Define the list as a constant variable
{% set patterns_list = ['phone', 'email', 'address', 'passport', 'credit', 'card', 'first_name', 'last_name', 'middle_name', 'surname'] %}

-- List sensitive column base on a list
with base as (
    select
        table_catalog as database_name,
        table_schema as schema_name,
        table_name,
        column_name,
        data_type,
        character_maximum_length,
        is_nullable
    from pg_catalog.svv_columns
    where
        table_schema not like 'staging_%'
        and table_name not like '_v_%'
        and data_type = 'character varying'
)
select *
from base
where
    {{ generate_like_statements("column_name", patterns_list) }}
order by schema_name, table_name, column_name
