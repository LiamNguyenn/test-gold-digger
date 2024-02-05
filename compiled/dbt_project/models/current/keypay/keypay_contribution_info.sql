
    with source as (

select * from "dev"."int__keypay"."contribution_info"

),

renamed as (

select
            "id",
            "cont_amount",
            "cont_type",
            "super_member_id",
            "employee_id",
            "failed",
            "_transaction_date",
            "_etl_date",
            "_modified",
            "_file"
from source

)

select *
from renamed