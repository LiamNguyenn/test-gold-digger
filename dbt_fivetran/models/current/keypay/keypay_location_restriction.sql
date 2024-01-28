{{ config(alias='location_restriction', materialized = 'view') }}
    with source as (

select * from {{ ref('int__keypay__location_restriction') }}

),

renamed as (

select
            "id",
            "business_id",
            "user_id",
            "filter_type",
            "value",
            "permissions",
            "_transaction_date",
            "_etl_date",
            "_modified",
            "_file"
from source

)

select *
from renamed
