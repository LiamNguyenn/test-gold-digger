{{ config(alias='employment_type', materialized = 'view') }}
    with source as (

select * from {{ ref('int__keypay__employment_type') }}

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
