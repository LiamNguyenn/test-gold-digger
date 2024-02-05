
    with source as (

select * from "dev"."int__keypay"."payrun_total_history"

),

renamed as (

select
            id,
            employee_id,
            payrun_id,
            total_hours,
            gross_earnings,
            net_earnings,
            is_excluded_from_billing,
            "_file",
            "_transaction_date",
            "_etl_date",
            "_modified"
from source

)

select *
from renamed