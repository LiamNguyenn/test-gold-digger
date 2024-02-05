
with source as (

    select * from "dev"."keypay_s3"."user_reseller"

),

renamed as (

    select
        userid::bigint          as userid,
        reseller_id::bigint     as reseller_id,
        _file::varchar          as _file,
        _transaction_date::date as _transaction_date,
        _etl_date::timestamp    as _etl_date,
        _modified::timestamp    as _modified,
        user_id::varchar        as user_id
    from source

)

select * from renamed