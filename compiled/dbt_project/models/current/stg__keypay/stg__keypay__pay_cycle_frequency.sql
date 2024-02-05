
with source as (

    select * from "dev"."keypay_s3"."pay_cycle_frequency"

),

renamed as (

    select
        id::bigint              as id,  -- noqa: RF04
        description::varchar    as description,
        _file::varchar          as _file,
        _transaction_date::date as _transaction_date,
        _etl_date::timestamp    as _etl_date,
        _modified::timestamp    as _modified
    from source

)

select * from renamed