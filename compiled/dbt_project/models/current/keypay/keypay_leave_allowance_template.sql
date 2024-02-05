
    with source as (

select * from "dev"."int__keypay"."leave_allowance_template"

),

renamed as (

select
            "id",
            "business_id",
            "name",
            "external_reference_id",
            "source",
            "business_award_package_id",
            "leave_accrual_start_date_type",
            "leave_year_start",
            "leave_loading_calculated_from_pay_category_id",
            "_transaction_date",
            "_etl_date",
            "_modified",
            "_file"
from source

)

select *
from renamed