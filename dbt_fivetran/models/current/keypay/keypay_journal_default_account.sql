{{ config(alias='journal_default_account', materialized = 'view') }}
    with source as (

select * from {{ ref('int__keypay__journal_default_account') }}

),

renamed as (

select
            "_transaction_date",
            "_modified",
            "id",
            "business_id",
            "account_type",
            "_etl_date",
            "_file"
from source

)

select *
from renamed
