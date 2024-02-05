
    with source as (

select * from "dev"."int__keypay"."employment_type"

),

renamed as (

select
            "id",
            "description",
            "_transaction_date",
            "_etl_date",
            "_modified",
            "_file"
from source

)

select *
from renamed