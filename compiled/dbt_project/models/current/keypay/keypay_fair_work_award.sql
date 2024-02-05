
    with source as (

select * from "dev"."int__keypay"."fair_work_award"

),

renamed as (

select
            "id",
            "name",
            "_transaction_date",
            "_etl_date",
            "_modified",
            "_file"
from source

)

select *
from renamed