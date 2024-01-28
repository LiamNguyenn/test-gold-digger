{{ config(alias='suburb', materialized = 'view') }}
    with source as (

select * from {{ ref('int__keypay_dwh__suburb') }}

),

renamed as (

select
            "id",
            "name",
            "postcode",
            "state",
            "country",
            "_transaction_date",
            "_etl_date",
            "_modified",
            "_file"
from source

)

select *
from renamed
