{{ config(alias='industry', materialized = 'view') }}
    with source as (

select * from {{ ref('int__keypay__industry') }}

),

renamed as (

select
            "id",
            "name",
            "_transaction_date",
            "_etl_date",
            "_modified",
            "_file"
from source

)

select *
from renamed
