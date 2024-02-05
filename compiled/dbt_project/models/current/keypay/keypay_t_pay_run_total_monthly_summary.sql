
    with source as (

select * from "dev"."mart__keypay"."_t_pay_run_total_monthly_summary"

),

renamed as (

select
            "employee_id",
            "business_id",
            "invoice_id",
            "billing_month",
            "is_excluded_from_billing",
            "monthly_gross_earnings",
            "monthly_net_earnings",
            "total_hours"
from source

)

select *
from renamed