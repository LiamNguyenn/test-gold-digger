{{ config(alias='user_report_access', materialized = 'view') }}
    with source as (

select * from {{ ref('int__keypay__user_report_access') }}

),

renamed as (

select
            "id",
            "user_id",
            "business_id",
            "access_type",
            "no_reporting_restriction",
            "_transaction_date",
            "_etl_date",
            "_modified",
            "_file"
from source

)

select *
from renamed
