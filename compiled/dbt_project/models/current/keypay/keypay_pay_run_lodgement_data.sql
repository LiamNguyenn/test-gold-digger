
    with source as (

select * from "dev"."int__keypay"."pay_run_lodgement_data"

),

renamed as (

select
            "id",
            "status",
            "is_test",
            "_transaction_date",
            "_etl_date",
            "_modified",
            "_file"
from source

)

select *
from renamed