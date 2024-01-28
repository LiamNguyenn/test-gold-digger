{{ config(alias='superfund_ato', materialized = 'view') }}
    with source as (

select * from {{ ref('int__keypay__superfund_ato') }}

),

renamed as (

select
            "abn",
            "fund_name",
            "usi",
            "product_name",
            "contribution_restrictions",
            "from_date",
            "to_date",
            "_transaction_date",
            "_etl_date"
from source

)

select *
from renamed
