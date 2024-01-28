{{ config(alias='deduction_category', materialized = 'view') }}
    with source as (

select * from {{ ref('int__keypay__deduction_category') }}

),

renamed as (

select
            "id",
            "deduction_category_name",
            "business_id",
            "tax_exempt",
            "is_deleted",
            "source",
            "external_reference_id",
            "payment_summary_classification_id",
            "expense_general_ledger_mapping_code",
            "liability_general_ledger_mapping_code",
            "sgc_calculation_impact",
            "minimum_wage_deduction_impact",
            "is_system",
            "deduction_category_ext_sg_id",
            "deduction_category_ext_uk_id",
            "is_resc",
            "is_name_read_only",
            "is_allow_pre_tax_super",
            "is_allow_member_voluntary",
            "_transaction_date",
            "_etl_date",
            "_modified",
            "_file"
from source

)

select *
from renamed
