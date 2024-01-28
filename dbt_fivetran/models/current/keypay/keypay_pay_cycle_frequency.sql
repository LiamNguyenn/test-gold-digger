{{ config(alias='pay_cycle_frequency', materialized = 'view') }}
    with source as (

select * from {{ ref('int__keypay__pay_cycle_frequency') }}

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
