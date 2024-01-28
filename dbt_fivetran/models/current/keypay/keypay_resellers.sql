{{ config(alias='resellers', materialized = 'view') }}
    with source as (

select * from {{ ref('int__keypay__resellers') }}

),

renamed as (

select
            "id",
            "name",
            "billing_name",
            "date_created_utc",
            "commence_billing_from",
            "_file",
            "_transaction_date",
            "_etl_date",
            "_modified"
from source

)

select *
from renamed
