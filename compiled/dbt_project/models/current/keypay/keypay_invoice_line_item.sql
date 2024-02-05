
    with source as (

select * from "dev"."int__keypay"."invoice_line_item"

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