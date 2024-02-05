
    with source as (

select * from "dev"."int__keypay"."leave_accrual"

),

renamed as (

select
            "id",
            "employee_id",
            "accrued_amount",
            "accrual_status_id",
            "_transaction_date",
            "_etl_date",
            "_modified",
            "_file"
from source

)

select *
from renamed