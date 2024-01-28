{{ config(alias='super_fund_product', materialized = 'view') }}
    with source as (

select * from {{ ref('int__keypay__super_fund_product') }}

),

renamed as (

select
            "id",
            "abn",
            "product_code",
            "product_type",
            "business_name",
            "product_name",
            "source",
            "business_id",
            "super_stream_status",
            "is_managed_by_system",
            "_transaction_date",
            "_etl_date",
            "_modified",
            "_file"
from source

)

select *
from renamed
