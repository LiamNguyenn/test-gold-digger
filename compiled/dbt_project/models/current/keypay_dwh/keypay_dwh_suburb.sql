
    with source as (

select * from "dev"."int__keypay_dwh"."suburb"

),

renamed as (

select
            "id",
            "name",
            "postcode",
            "state",
            "country",
            "_transaction_date",
            "_etl_date",
            "_modified",
            "_file"
from source

)

select *
from renamed