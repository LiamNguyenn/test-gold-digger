
with source as (

    select * from "dev"."keypay_s3"."user_employee"

),

renamed as (

    select
        user_id::bigint         as user_id,
        employee_id::bigint     as employee_id,
        _file::varchar          as _file,
        _transaction_date::date as _transaction_date,
        _etl_date::timestamp    as _etl_date,
        _modified::timestamp    as _modified
    from source

)

select * from renamed