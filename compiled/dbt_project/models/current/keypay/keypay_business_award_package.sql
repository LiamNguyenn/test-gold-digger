
    with source as (

select * from "dev"."int__keypay"."business_award_package"

),

renamed as (

select
            "id",
            "business_id",
            "award_package_id",
            "current_version_id",
            "award_package_name",
            "installation_status",
            "installation_status_last_updated_utc",
            "_transaction_date",
            "_etl_date",
            "_modified",
            "_file"
from source

)

select *
from renamed