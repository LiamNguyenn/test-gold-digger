
    with source as (

select * from "dev"."int__keypay"."bacs_details"

),

renamed as (

select
            "id",
            "businessid",
            "_transaction_date",
            "_etl_date",
            "_modified",
            "_file"
from source

)

select *
from renamed