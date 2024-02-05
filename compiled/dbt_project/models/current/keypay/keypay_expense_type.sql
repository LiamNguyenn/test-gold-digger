
    with source as (

select * from "dev"."int__keypay"."expense_type"

),

renamed as (

select
            "id",
            "description",
            "unit_cost",
            "_transaction_date",
            "_etl_date",
            "_modified",
            "_file"
from source

)

select *
from renamed