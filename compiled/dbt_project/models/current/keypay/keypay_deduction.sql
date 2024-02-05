
    with source as (

select * from "dev"."int__keypay"."deduction"

),

renamed as (

select
            "id",
            "employee_id",
            "pay_run_total_id",
            "deduction_category_id",
            "amount",
            "pay_run_id",
            "employee_super_fund_id",
            "contribution_info_id",
            "associated_employee_deduction_category_id",
            "bank_account_id",
            "is_resc",
            "bank_account_bsb",
            "bank_account_number",
            "bank_account_type",
            "is_member_voluntary",
            "associated_employee_pension_contribution_plan_id",
            "is_pension_scheme_salary_sacrifice",
            "additional_data",
            "paid_to_tax_office",
            "payg_adjustment_id",
            "bank_account_swift",
            "bank_account_branch_code",
            "_transaction_date",
            "_etl_date",
            "_modified",
            "_file"
from source

)

select *
from renamed