
    with source as (

select * from "dev"."int__keypay"."statutory_settings"

),

renamed as (

select
            "id",
            "business_id",
            "income_tax_number_encrypted",
            "e_number",
            "epf_number",
            "socso_number",
            "hrdf_status",
            "_transaction_date",
            "_etl_date",
            "_modified",
            "_file"
from source

)

select *
from renamed