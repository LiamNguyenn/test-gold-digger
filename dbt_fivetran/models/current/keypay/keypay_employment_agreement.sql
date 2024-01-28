{{ config(alias='employment_agreement', materialized = 'view') }}
    with source as (

select * from {{ ref('int__keypay__employment_agreement') }}

),

renamed as (

select
            "id",
            "business_id",
            "classification",
            "date_created_utc",
            "external_reference_id",
            "is_deleted",
            "business_award_package_id",
            "_transaction_date",
            "_etl_date",
            "_modified",
            "_file"
from source

)

select *
from renamed
