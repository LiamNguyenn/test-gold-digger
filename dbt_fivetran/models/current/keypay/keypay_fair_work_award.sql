{{ config(alias='fair_work_award', materialized = 'view') }}
    with source as (

select * from {{ ref('int__keypay__fair_work_award') }}

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
