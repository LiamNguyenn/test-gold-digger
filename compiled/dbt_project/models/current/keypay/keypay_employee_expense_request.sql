
    with source as (

select * from "dev"."int__keypay"."employee_expense_request"

),

renamed as (

select
            "id",
            "employee_id",
            "status",
            "created_by_user_id",
            "date_created_utc",
            "status_updated_by_user_id",
            "date_status_updated_utc",
            "description",
            "status_update_notes",
            "pay_run_total_id",
            "business_id",
            "date_first_approved_utc",
            "_transaction_date",
            "_etl_date",
            "_modified",
            "_file"
from source

)

select *
from renamed