{{ config(alias='location', materialized = 'view') }}
with source as (

    select * from {{ source('keypay_s3', 'location') }}

),

renamed as (

    select
        id::bigint              as id,  -- noqa: RF04
        name::varchar           as name,  -- noqa: RF04
        businessid::bigint      as businessid,
        is_deleted::boolean     as is_deleted,
        parentid::bigint        as parentid,
        date_created::varchar   as date_created,
        _file::varchar          as _file,
        _transaction_date::date as _transaction_date,
        _etl_date::timestamp    as _etl_date,
        _modified::timestamp    as _modified
    from source

)

select * from renamed
