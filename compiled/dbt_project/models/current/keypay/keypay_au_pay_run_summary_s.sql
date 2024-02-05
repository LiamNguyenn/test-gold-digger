
    with source as (

select * from "dev"."mart__keypay"."au_pay_run_summary_s"

),

renamed as (

select
            "employee_id",
            "residential_state",
            "business_id",
            "industry",
            "business_billed_employees",
            "invoice_id",
            "billing_month",
            "is_excluded_from_billing",
            "monthly_gross_earnings",
            "monthly_net_earnings",
            "total_hours",
            "hourly_rate",
            "gender",
            "age",
            "employment_type",
            "start_date",
            "end_date",
            "z_score_earnings",
            "z_score_hours",
            "z_score_hourly_rate"
from source

)

select *
from renamed