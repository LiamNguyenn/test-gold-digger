{{ config(alias='pay_cycle', materialized = 'view') }}
    with source as (

select * from {{ ref('int__keypay__pay_cycle') }}

),

renamed as (

select
            "id",
            "business_id",
            "pay_cycle_frequencyid",
            "name",
            "last_pay_run",
            "is_deleted",
            "aba_detailsid",
            "_transaction_date",
            "_etl_date",
            "_modified",
            "_file"
from source

)

select *
from renamed
