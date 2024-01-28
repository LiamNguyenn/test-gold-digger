{{ config(alias='leave_request', materialized = 'view') }}
    with source as (

select * from {{ ref('int__keypay__leave_request') }}

),

renamed as (

select
            "id",
            "employee_id",
            "from_date",
            "to_date",
            "total_hours",
            "requested_date",
            "status",
            "business_id",
            "leave_category_id",
            "_transaction_date",
            "_etl_date",
            "_modified",
            "_file"
from source

)

select *
from renamed
