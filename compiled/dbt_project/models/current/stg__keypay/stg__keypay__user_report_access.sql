
with source as (

    select * from "dev"."keypay_s3"."user_report_access"

),

renamed as (

    select
        id::bigint                       as id,  -- noqa: RF04
        user_id::bigint                  as user_id,
        business_id::bigint              as business_id,
        access_type::bigint              as access_type,
        no_reporting_restriction::bigint as no_reporting_restriction,
        _file::varchar                   as _file,
        _transaction_date::date          as _transaction_date,
        _etl_date::timestamp             as _etl_date,
        _modified::timestamp             as _modified
    from source

)

select * from renamed