-- Define the list as a constant variable


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
    column_name like '%phone%'
or column_name like '%email%'
or column_name like '%address%'
or column_name like '%passport%'
or column_name like '%credit%'
or column_name like '%card%'
or column_name like '%first_name%'
or column_name like '%last_name%'
or column_name like '%middle_name%'
or column_name like '%surname%'
order by schema_name, table_name, column_name