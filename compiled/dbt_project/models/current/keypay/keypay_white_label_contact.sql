
    with source as (

select * from "dev"."int__keypay"."white_label_contact"

),

renamed as (

select
            "id",
            "white_label_id",
            "user_id",
            "contact_type",
            "name",
            "email",
            "_transaction_date",
            "_etl_date",
            "_modified",
            "_file"
from source

)

select *
from renamed