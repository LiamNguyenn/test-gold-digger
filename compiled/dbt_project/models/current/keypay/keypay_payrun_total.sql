
    with source as (

select * from "dev"."int__keypay"."payrun_total"

),

renamed as (

select
            "id",
            "employee_id",
            "payrun_id",
            "total_hours",
            "gross_earnings",
            "net_earnings",
            "is_excluded_from_billing",
            "_transaction_date",
            "_etl_date",
            "_modified",
            "_file"
from source

)

select *
from renamed