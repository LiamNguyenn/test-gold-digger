
    with source as (

select * from "dev"."int__keypay"."earnings_line"

),

renamed as (

select
            "id",
            "employee_id",
            "pay_category_id",
            "pay_run_id",
            "units",
            "location_id",
            "pay_run_total_id",
            "rate",
            "earnings_line_status_id",
            "external_reference_id",
            "net_earnings",
            "net_earnings_reporting",
            "earnings_line_ext_au_id",
            "_file",
            "_transaction_date",
            "_etl_date",
            "_modified"
from source

)

select *
from renamed