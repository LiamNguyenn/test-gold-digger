{{ config(alias='device_token', materialized = 'view') }}
with source as (

    select * from {{ source('keypay_s3', 'device_token') }}

),

renamed as (

    select
        id::bigint                as id,  -- noqa: RF04
        user_id::bigint           as user_id,
        platform::bigint          as platform,
        endpoint::varchar         as endpoint,
        date_created_utc::varchar as date_created_utc,
        _file::varchar            as _file,
        _transaction_date::date   as _transaction_date,
        _etl_date::timestamp      as _etl_date,
        _modified::timestamp      as _modified
    from source

)

select * from renamed
