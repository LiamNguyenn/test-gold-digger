{{ config(alias='employee_expense', materialized = 'view') }}
    with source as (

select * from {{ ref('int__keypay__employee_expense') }}

),

renamed as (

select
            "id",
            "pay_run_id",
            "pay_run_total_id",
            "employee_id",
            "employee_expense_category_id",
            "location_id",
            "business_id",
            "amount",
            "notes",
            "external_id",
            "employee_recurring_expense_id",
            "employee_expense_request_id",
            "tax_code",
            "tax_rate",
            "tax_code_display_name",
            "_transaction_date",
            "_etl_date",
            "_modified",
            "_file"
from source

)

select *
from renamed
