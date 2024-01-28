{{ config(alias='super_fund_product', materialized = 'view') }}
with source as (

    select * from {{ source('keypay_s3', 'super_fund_product') }}

),

renamed as (

    select
        id::bigint                    as id,  -- noqa: RF04
        abn::varchar                  as abn,
        product_code::varchar         as product_code,
        product_type::varchar         as product_type,
        business_name::varchar        as business_name,
        product_name::varchar         as product_name,
        source::bigint                as source,  -- noqa: RF04
        business_id::bigint           as business_id,
        super_stream_status::bigint   as super_stream_status,
        is_managed_by_system::boolean as is_managed_by_system,
        _file::varchar                as _file,
        _transaction_date::date       as _transaction_date,
        _etl_date::timestamp          as _etl_date,
        _modified::timestamp          as _modified
    from source

)

select * from renamed
