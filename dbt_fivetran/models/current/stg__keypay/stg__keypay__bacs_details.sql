{{ config(alias='bacs_details', materialized = 'view') }}
with source as (

    select * from {{ source('keypay_s3', 'bacs_details') }}

),

renamed as (

    select
        id::bigint              as id,  -- noqa: RF04
        businessid::bigint      as businessid,
        _file::varchar          as _file,
        _transaction_date::date as _transaction_date,
        _etl_date::timestamp    as _etl_date,
        _modified::timestamp    as _modified
    from source

)

select * from renamed
