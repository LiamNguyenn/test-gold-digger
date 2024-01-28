{{ config(alias='user_whitelabel', materialized = 'view') }}
    with source as (

select * from {{ ref('int__keypay__user_whitelabel') }}

),

renamed as (

select
            "user_id",
            "whitelabel_id",
            "is_default_parent",
            "_transaction_date",
            "_etl_date",
            "_modified",
            "_file"
from source

)

select *
from renamed
