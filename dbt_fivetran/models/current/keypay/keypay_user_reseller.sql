{{ config(alias='user_reseller', materialized = 'view') }}
    with source as (

select * from {{ ref('int__keypay__user_reseller') }}

),

renamed as (

select
            "userid",
            "reseller_id",
            "_transaction_date",
            "_etl_date",
            "_modified",
            "_file"
from source

)

select *
from renamed
