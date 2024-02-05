
with source as (

    select * from "dev"."keypay_s3"."employee_history"

),

renamed as (

    select
        id::bigint                         as id,  -- noqa: RF04
        employee_id::bigint                as employee_id,
        employee_history_action_id::bigint as employee_history_action_id,
        date_created::varchar              as date_created,
        _file::varchar                     as _file,
        _transaction_date::date            as _transaction_date,
        _etl_date::timestamp               as _etl_date,
        _modified::timestamp               as _modified
    from source

)

select * from renamed