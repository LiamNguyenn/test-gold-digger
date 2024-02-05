
    with source as (

select * from "dev"."int__keypay"."user_business"

),

renamed as (

select
            "user_id",
            "business_id",
            "is_single_sign_on_enabled",
            "_transaction_date",
            "_etl_date",
            "_modified",
            "_file"
from source

)

select *
from renamed