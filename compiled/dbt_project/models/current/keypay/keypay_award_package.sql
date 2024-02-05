
    with source as (

select * from "dev"."int__keypay"."award_package"

),

renamed as (

select
            "id",
            "name",
            "date_created_utc",
            "fair_work_award_id",
            "is_disabled",
            "_transaction_date",
            "_etl_date",
            "_modified",
            "_file"
from source

)

select *
from renamed