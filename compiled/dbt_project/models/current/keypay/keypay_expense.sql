
    with source as (

select * from "dev"."int__keypay"."expense"

),

renamed as (

select
            "id",
            "expense_date",
            "business_id",
            "unit_cost",
            "quantity",
            "invoice_id",
            "notes",
            "expense_type",
            "displayed_unit_cost",
            "_transaction_date",
            "_etl_date",
            "_modified",
            "_file"
from source

)

select *
from renamed