{{ config(alias='invoice', materialized = 'view') }}
with source as (

    select * from {{ source('keypay_s3', 'invoice') }}

),

renamed as (

    select
        id::bigint                as id,  -- noqa: RF04
        date::varchar             as date,  -- noqa: RF04
        gst_rate::float           as gst_rate,
        billing_region_id::bigint as billing_region_id,
        invoicee_id::bigint       as invoicee_id,
        invoicee_type_id::bigint  as invoicee_type_id,
        _file::varchar            as _file,
        _transaction_date::date   as _transaction_date,
        _etl_date::timestamp      as _etl_date,
        _modified::timestamp      as _modified
    from source

)

select * from renamed
