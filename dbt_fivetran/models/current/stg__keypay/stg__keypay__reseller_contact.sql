{{ config(alias='reseller_contact', materialized = 'view') }}
with source as (

    select * from {{ source('keypay_s3', 'reseller_contact') }}

),

renamed as (

    select
        id::varchar             as id,  -- noqa: RF04
        reseller_id::varchar    as reseller_id,
        user_id::varchar        as user_id,
        contact_type::varchar   as contact_type,
        name::varchar           as name,  -- noqa: RF04
        email::varchar          as email,
        _file::varchar          as _file,
        _transaction_date::date as _transaction_date,
        _etl_date::timestamp    as _etl_date,
        _modified::timestamp    as _modified
    from source

)

select * from renamed
