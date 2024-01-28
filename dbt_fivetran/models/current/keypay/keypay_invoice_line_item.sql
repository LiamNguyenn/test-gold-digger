{{ config(alias='invoice_line_item', materialized = 'view') }}
    with source as (

select * from {{ ref('int__keypay__invoice_line_item') }}

),

renamed as (

select
            "id",
            "invoice_id",
            "total_including_gst",
            "abn",
            "business_id",
            "unit_price_including_gst",
            "quantity",
            "billing_code",
            "billing_plan",
            "_transaction_date",
            "_etl_date",
            "_modified",
            "_file"
from source

)

select *
from renamed
