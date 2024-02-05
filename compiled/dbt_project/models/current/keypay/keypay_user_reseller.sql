
    with source as (

select * from "dev"."int__keypay"."user_reseller"

),

renamed as (

select
            "userid",
            "reseller_id",
            "_transaction_date",
            "_etl_date",
            "_modified",
            "_file"
from source

)

select *
from renamed