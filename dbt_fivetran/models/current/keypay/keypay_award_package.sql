{{ config(alias='award_package', materialized = 'view') }}
    with source as (

select * from {{ ref('int__keypay__award_package') }}

),

renamed as (

select
            "id",
            "name",
            "date_created_utc",
            "fair_work_award_id",
            "is_disabled",
            "_transaction_date",
            "_etl_date",
            "_modified",
            "_file"
from source

)

select *
from renamed
