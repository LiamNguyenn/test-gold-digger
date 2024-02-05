
    with source as (

select * from "dev"."int__keypay"."device_token"

),

renamed as (

select
            "id",
            "user_id",
            "platform",
            "endpoint",
            "date_created_utc",
            "_transaction_date",
            "_etl_date",
            "_modified",
            "_file"
from source

)

select *
from renamed