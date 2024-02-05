
with source as (

    select * from "dev"."keypay_s3"."invoice_line_item"

),

renamed as (

    select
        id::bigint                        as id,  -- noqa: RF04
        invoice_id::bigint                as invoice_id,
        total_including_gst::float        as total_including_gst,
        abn::varchar                      as abn,
        business_id::bigint               as business_id,
        quantity::float                   as quantity,
        billing_code::varchar             as billing_code,
        billing_plan::varchar             as billing_plan,
        _file::varchar                    as _file,
        _transaction_date::date           as _transaction_date,
        _etl_date::timestamp              as _etl_date,
        _modified::timestamp              as _modified,
        unit_price_including_gst::varchar as unit_price_including_gst
    from source

)

select * from renamed