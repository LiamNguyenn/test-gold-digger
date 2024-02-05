
    with source as (

select * from "dev"."int__keypay"."employee_history"

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