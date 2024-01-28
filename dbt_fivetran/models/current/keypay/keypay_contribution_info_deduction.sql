{{ config(alias='contribution_info_deduction', materialized = 'view') }}
    with source as (

select * from {{ ref('int__keypay__contribution_info_deduction') }}

),

renamed as (

select
            "contribution_info_id",
            "deduction_id",
            "failed",
            "id",
            "_transaction_date",
            "_etl_date",
            "_modified",
            "_file"
from source

)

select *
from renamed
