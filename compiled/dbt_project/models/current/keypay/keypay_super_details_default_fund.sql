
    with source as (

select * from "dev"."int__keypay"."super_details_default_fund"

),

renamed as (

select
            "id",
            "super_details_id",
            "usi",
            "abn",
            "name",
            "is_deleted",
            "_transaction_date",
            "_etl_date",
            "_modified",
            "_file"
from source

)

select *
from renamed