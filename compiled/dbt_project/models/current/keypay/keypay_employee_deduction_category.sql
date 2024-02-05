
    with source as (

select * from "dev"."int__keypay"."employee_deduction_category"

),

renamed as (

select
            "id",
            "employee_id",
            "deduction_category_id",
            "from_date",
            "to_date",
            "amount",
            "employee_super_fund_id",
            "expiry_date",
            "maximum_amount_paid",
            "is_active",
            "bank_account_id",
            "deleted",
            "notes",
            "external_reference_id",
            "source",
            "deduction_type",
            "preserved_earnings",
            "preserved_earnings_amount",
            "preserved_earnings_amount_not_reached_action",
            "carry_forward_unpaid_deductions",
            "payment_reference",
            "employee_pension_contribution_plan_id",
            "additional_data",
            "paid_to_tax_office",
            "priority",
            "student_loan_deduction_option",
            "carry_forward_unused_preserved_earnings",
            "tiered_deduction_settings_id",
            "paid_to_external_service",
            "employee_deduction_category_ext_sg_id",
            "employee_deduction_category_ext_my_id",
            "_transaction_date",
            "_etl_date",
            "_modified",
            "_file"
from source

)

select *
from renamed