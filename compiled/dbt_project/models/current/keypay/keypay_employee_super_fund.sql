
    with source as (

select * from "dev"."int__keypay"."employee_super_fund"

),

renamed as (

select
            "id",
            "super_fund_name",
            "member_number",
            "allocated_percentage",
            "fixed_amount",
            "employee_id",
            "deleted",
            "super_fund_product_id",
            "allocate_balance",
            "has_non_super_stream_compliant_fund",
            "date_employee_nominated_utc",
            "super_details_default_fund_id",
            "self_managed_super_fund_id",
            "_transaction_date",
            "_etl_date",
            "_modified",
            "_file"
from source

)

select *
from renamed