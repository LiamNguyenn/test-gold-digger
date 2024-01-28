{{ config(alias='fair_work_award_selection', materialized = 'view') }}
    with source as (

select * from {{ ref('int__keypay__fair_work_award_selection') }}

),

renamed as (

select
            "id",
            "fair_work_award_id",
            "business_id",
            "date_time_utc",
            "source",
            "_transaction_date",
            "_etl_date",
            "_modified",
            "_file"
from source

)

select *
from renamed
