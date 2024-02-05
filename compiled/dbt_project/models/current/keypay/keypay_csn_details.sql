
    with source as (

select * from "dev"."int__keypay"."csn_details"

),

renamed as (

select
            "id",
            "business_id",
            "cpf_submission_number",
            "csn_type",
            "is_deleted",
            "_transaction_date",
            "_etl_date",
            "_modified",
            "_file"
from source

)

select *
from renamed