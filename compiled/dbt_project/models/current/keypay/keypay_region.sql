
    with source as (

select * from "dev"."int__keypay"."region"

),

renamed as (

select
            "id",
            "currency",
            "name",
            "culture_name",
            "default_standard_hours_per_day",
            "commence_billing_from",
            "minimum_bill_able_amount",
            "_file",
            "_transaction_date",
            "_etl_date",
            "_modified"
from source

)

select *
from renamed