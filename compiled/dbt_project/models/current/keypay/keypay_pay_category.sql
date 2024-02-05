
    with source as (

select * from "dev"."int__keypay"."pay_category"

),

renamed as (

select
            "id",
            "pay_category_name",
            "business_id",
            "date_created",
            "rate_unit_id",
            "accrues_leave",
            "default_super_rate",
            "is_tax_exempt",
            "linked_pay_category_id",
            "penalty_loading_multiplier",
            "is_deleted",
            "rate_loading_multiplier",
            "external_reference_id",
            "source",
            "is_payroll_tax_exempt",
            "pay_category_type",
            "business_award_package_id",
            "payment_summary_classification_id",
            "general_ledger_mapping_code",
            "super_liability_mapping_code",
            "super_expense_mapping_code",
            "is_w1_exempt",
            "number_of_decimal_places",
            "is_national_insurance_exempt",
            "minimum_wage_calculation_impact",
            "exclude_from_average_earnings",
            "cpf_classification_id",
            "include_in_gross_rate_of_pay",
            "exclude_from_ordinary_earnings",
            "hide_units_on_pay_slip",
            "pay_category_ext_my_id",
            "rounding_method",
            "pay_category_ext_nz_id",
            "pay_category_ext_uk_id",
            "allowance_description",
            "pay_category_ext_sg_id",
            "_transaction_date",
            "_etl_date",
            "_modified",
            "_file"
from source

)

select *
from renamed