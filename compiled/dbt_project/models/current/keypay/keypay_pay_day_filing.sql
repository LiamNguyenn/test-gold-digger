
    with source as (

select * from "dev"."int__keypay"."pay_day_filing"

),

renamed as (

select
            "id",
            "business_id",
            "pay_run_id",
            "status",
            "date_last_modified",
            "date_submitted",
            "version",
            "pay_day_filing_lodgement_data_id",
            "_transaction_date",
            "_etl_date",
            "_modified",
            "_file"
from source

)

select *
from renamed