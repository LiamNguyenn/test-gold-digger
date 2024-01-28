{{ config(alias='super_details', materialized = 'view') }}
    with source as (

select * from {{ ref('int__keypay__super_details') }}

),

renamed as (

select
            "id",
            "business_id",
            "date_registered_utc",
            "enabled",
            "date_beam_terms_accepted_utc",
            "_transaction_date",
            "_etl_date",
            "_modified",
            "_file"
from source

)

select *
from renamed
