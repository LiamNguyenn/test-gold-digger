
    with source as (

select * from "dev"."int__keypay"."tax_file_declaration"

),

renamed as (

select
            "id",
            "employee_id",
            "employment_type_id",
            "from_date",
            "to_date",
            "_transaction_date",
            "_etl_date",
            "_modified",
            "_file"
from source

)

select *
from renamed