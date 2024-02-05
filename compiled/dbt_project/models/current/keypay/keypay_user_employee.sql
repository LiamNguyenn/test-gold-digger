
    with source as (

select * from "dev"."int__keypay"."user_employee"

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