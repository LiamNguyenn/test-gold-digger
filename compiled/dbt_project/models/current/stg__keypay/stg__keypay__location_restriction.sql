
with source as (

    select * from "dev"."keypay_s3"."location_restriction"

),

renamed as (

    select
        id::bigint              as id,  -- noqa: RF04
        business_id::bigint     as business_id,
        user_id::bigint         as user_id,
        filter_type::bigint     as filter_type,
        value::varchar          as value,  -- noqa: RF04
        "permissions"::bigint   as "permissions",
        _file::varchar          as _file,
        _transaction_date::date as _transaction_date,
        _etl_date::timestamp    as _etl_date,
        _modified::timestamp    as _modified
    from source

)

select * from renamed