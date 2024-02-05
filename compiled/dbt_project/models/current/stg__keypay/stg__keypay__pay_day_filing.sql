
with source as (

    select * from "dev"."keypay_s3"."pay_day_filing"

),

renamed as (

    select
        id::bigint                               as id,  -- noqa: RF04
        business_id::bigint                      as business_id,
        pay_run_id::bigint                       as pay_run_id,
        status::bigint                           as status,
        date_last_modified::varchar              as date_last_modified,
        date_submitted::varchar                  as date_submitted,
        version::bigint                          as version,  -- noqa: RF04
        pay_day_filing_lodgement_data_id::bigint as pay_day_filing_lodgement_data_id,
        _file::varchar                           as _file,
        _transaction_date::date                  as _transaction_date,
        _etl_date::timestamp                     as _etl_date,
        _modified::timestamp                     as _modified
    from source

)

select * from renamed