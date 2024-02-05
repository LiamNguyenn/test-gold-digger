
    with source as (

select * from "dev"."int__keypay"."billing_plan"

),

renamed as (

select
            "id",
            "name",
            "function_employee_onboarding",
            "price_per_unit",
            "_transaction_date",
            "_etl_date",
            "_modified",
            "_file"
from source

)

select *
from renamed