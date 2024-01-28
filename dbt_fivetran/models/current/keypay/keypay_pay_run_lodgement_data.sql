{{ config(alias='pay_run_lodgement_data', materialized = 'view') }}
    with source as (

select * from {{ ref('int__keypay__pay_run_lodgement_data') }}

),

renamed as (

select
            "id",
            "status",
            "is_test",
            "_transaction_date",
            "_etl_date",
            "_modified",
            "_file"
from source

)

select *
from renamed
