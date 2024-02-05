
    with source as (

select * from "dev"."int__keypay"."user_whitelabel"

),

renamed as (

select
            "user_id",
            "whitelabel_id",
            "is_default_parent",
            "_transaction_date",
            "_etl_date",
            "_modified",
            "_file"
from source

)

select *
from renamed