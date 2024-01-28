{{ config(alias='super_payment', materialized = 'view') }}
    with source as (

select * from {{ ref('int__keypay__super_payment') }}

),

renamed as (

select
            "id",
            "pay_run_total_id",
            "employee_super_fund_id",
            "amount",
            "pay_run_id",
            "employee_id",
            "contribution_type",
            "_transaction_date",
            "_etl_date",
            "_modified",
            "_file"
from source

)

select *
from renamed
