{{ config(alias='rate_unit', materialized = 'view') }}
    with source as (

select * from {{ ref('int__keypay__rate_unit') }}

),

renamed as (

select
            "id",
            "description",
            "_transaction_date",
            "_etl_date",
            "_modified",
            "_file"
from source

)

select *
from renamed
