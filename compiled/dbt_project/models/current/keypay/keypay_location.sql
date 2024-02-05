
    with source as (

select * from "dev"."int__keypay"."location"

),

renamed as (

select
            "id",
            "name",
            "businessid",
            "is_deleted",
            "parentid",
            "date_created",
            "_transaction_date",
            "_etl_date",
            "_modified",
            "_file"
from source

)

select *
from renamed