
with source as (

    select * from "dev"."keypay_s3"."business_award_package"

),

renamed as (

    select
        id::bigint                                    as id,  -- noqa: RF04
        business_id::bigint                           as business_id,
        award_package_id::bigint                      as award_package_id,
        current_version_id::bigint                    as current_version_id,
        award_package_name::varchar                   as award_package_name,
        installation_status::bigint                   as installation_status,
        installation_status_last_updated_utc::varchar as installation_status_last_updated_utc,
        _file::varchar                                as _file,
        _transaction_date::date                       as _transaction_date,
        _etl_date::timestamp                          as _etl_date,
        _modified::timestamp                          as _modified
    from source

)

select * from renamed