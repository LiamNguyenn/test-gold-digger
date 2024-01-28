{{ config(alias='invoice', materialized = 'view') }}
    with source as (

select * from {{ ref('int__keypay__invoice') }}

),

renamed as (

select
            "id",
            "date",
            "gst_rate",
            "billing_region_id",
            "invoicee_id",
            "invoicee_type_id",
            "_transaction_date",
            "_etl_date",
            "_modified",
            "_file"
from source

)

select *
from renamed
