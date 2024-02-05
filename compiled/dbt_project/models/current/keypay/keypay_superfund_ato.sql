
    with source as (

select * from "dev"."int__keypay"."superfund_ato"

),

renamed as (

select
            "abn",
            "fund_name",
            "usi",
            "product_name",
            "contribution_restrictions",
            "from_date",
            "to_date",
            "_transaction_date",
            "_etl_date"
from source

)

select *
from renamed