{{ config(alias='aba_details', materialized = 'view') }}
    with source as (

select * from {{ ref('int__keypay__aba_details') }}

),

renamed as (

select
            "id",
            "businessid",
            "_transaction_date",
            "_etl_date",
            "_modified",
            "_file"
from source

)

select *
from renamed
