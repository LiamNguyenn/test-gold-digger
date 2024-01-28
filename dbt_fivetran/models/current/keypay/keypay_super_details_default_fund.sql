{{ config(alias='super_details_default_fund', materialized = 'view') }}
    with source as (

select * from {{ ref('int__keypay__super_details_default_fund') }}

),

renamed as (

select
            "id",
            "super_details_id",
            "usi",
            "abn",
            "name",
            "is_deleted",
            "_transaction_date",
            "_etl_date",
            "_modified",
            "_file"
from source

)

select *
from renamed
