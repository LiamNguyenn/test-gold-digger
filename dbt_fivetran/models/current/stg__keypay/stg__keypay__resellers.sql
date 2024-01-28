{{ config(alias='resellers', materialized = 'view') }}
with source as (

    select * from {{ source('keypay_s3', 'resellers') }}

),

renamed as (

    select
        id::bigint                     as id,  -- noqa: RF04
        name::varchar                  as name,  -- noqa: RF04
        billing_name::varchar          as billing_name,
        date_created_utc::varchar      as date_created_utc,
        commence_billing_from::varchar as commence_billing_from,
        _file::varchar                 as _file,
        _transaction_date::date        as _transaction_date,
        _etl_date::timestamp           as _etl_date,
        _modified::timestamp           as _modified,
        default_region_id::varchar     as default_region_id
    from source

)

select * from renamed
