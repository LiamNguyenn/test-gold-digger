{{ config(alias='earnings_line_ext_au', materialized = 'view') }}
    with source as (

select * from {{ ref('int__keypay__earnings_line_ext_au') }}

),

renamed as (

select
            "id",
            "earnings_line_id",
            "pay_run_id",
            "average_total_earnings_amount",
            "average_total_earnings_payg_amount",
            "average_additional_payments_amount",
            "full_earnings_payg_amount",
            "calculated_payg_amount",
            "max_payg_amount",
            "gross_earnings_amount",
            "gross_earnings_payg_amount",
            "pre_adjustment_payg_withholding_amount",
            "gross_earnings_stsl_amount",
            "average_total_earnings_stsl_amount",
            "full_earnings_stsl_amount",
            "calculated_stsl_amount",
            "lump_sum_e_financial_year",
            "_file",
            "_transaction_date",
            "_etl_date",
            "_modified"
from source

)

select *
from renamed
