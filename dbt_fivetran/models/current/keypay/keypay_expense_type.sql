{{ config(alias='expense_type', materialized = 'view') }}
    with source as (

select * from {{ ref('int__keypay__expense_type') }}

),

renamed as (

select
            "id",
            "description",
            "unit_cost",
            "_transaction_date",
            "_etl_date",
            "_modified",
            "_file"
from source

)

select *
from renamed
