{{ config(alias='pension_scheme_contribution_plan', materialized = 'view') }}
    with source as (

select * from {{ ref('int__keypay__pension_scheme_contribution_plan') }}

),

renamed as (

select
            "id",
            "employee_contribution_percentage",
            "employer_contribution_percentage",
            "pension_type",
            "max_earnings_threshold",
            "min_earnings_threshold",
            "contribution_group_name",
            "contribution_group_id",
            "contribution_plan_name",
            "reporting_frequency",
            "calculate_on_qualifying_earnings",
            "pension_scheme_id",
            "contribution_plan_id",
            "collection_source_id",
            "is_deleted",
            "salary_sacrifice_percentage",
            "salary_sacrifice_deduction_category_id",
            "nic_saving_rebate_percentage",
            "salary_sacrifice_pay_category_ids",
            "employee_contribution_pay_category_ids",
            "employer_contribution_pay_category_ids",
            "is_auto_enrolment_scheme",
            "lower_earnings_disregard",
            "lower_default_earnings_disregard_type",
            "upper_earnings_cap",
            "upper_default_earnings_cap_type",
            "use_tax_month_pay_period",
            "_transaction_date",
            "_etl_date",
            "_modified",
            "_file"
from source

)

select *
from renamed
