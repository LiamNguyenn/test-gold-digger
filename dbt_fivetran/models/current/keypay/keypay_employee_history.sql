{{ config(alias='employee_history', materialized = 'view') }}
    with source as (

select * from {{ ref('int__keypay__employee_history') }}

),

renamed as (

select
            "id",
            "employee_id",
            "employee_history_action_id",
            "date_created",
            "_transaction_date",
            "_etl_date",
            "_modified",
            "_file"
from source

)

select *
from renamed
