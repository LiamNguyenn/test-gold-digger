
    with source as (

select * from "dev"."int__keypay"."user_report_access"

),

renamed as (

select
            "id",
            "user_id",
            "business_id",
            "access_type",
            "no_reporting_restriction",
            "_transaction_date",
            "_etl_date",
            "_modified",
            "_file"
from source

)

select *
from renamed