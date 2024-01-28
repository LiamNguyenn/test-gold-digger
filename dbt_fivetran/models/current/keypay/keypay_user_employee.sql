{{ config(alias='user_employee', materialized = 'view') }}
    with source as (

select * from {{ ref('int__keypay__user_employee') }}

),

renamed as (

select
            "user_id",
            "employee_id",
            "_transaction_date",
            "_etl_date",
            "_modified",
            "_file"
from source

)

select *
from renamed
