
    with source as (

select * from "dev"."int__keypay"."contribution_info_deduction"

),

renamed as (

select
            "contribution_info_id",
            "deduction_id",
            "failed",
            "id",
            "_transaction_date",
            "_etl_date",
            "_modified",
            "_file"
from source

)

select *
from renamed