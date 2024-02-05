
with source as (

    select * from "dev"."keypay_s3"."employment_agreement"

),

renamed as (

    select
        id::bigint                        as id,  -- noqa: RF04
        business_id::bigint               as business_id,
        classification::varchar           as classification,
        date_created_utc::varchar         as date_created_utc,
        external_reference_id::bigint     as external_reference_id,
        is_deleted::boolean               as is_deleted,
        business_award_package_id::bigint as business_award_package_id,
        _file::varchar                    as _file,
        _transaction_date::date           as _transaction_date,
        _etl_date::timestamp              as _etl_date,
        _modified::timestamp              as _modified
    from source

)

select * from renamed