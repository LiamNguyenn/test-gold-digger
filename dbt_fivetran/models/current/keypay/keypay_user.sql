{{ config(alias='user', materialized = 'view') }}
    with source as (

select * from {{ ref('int__keypay__user') }}

),

renamed as (

select
            "id",
            "first_name",
            "last_name",
            "email",
            "is_active",
            "is_admin",
            "_transaction_date",
            "_etl_date",
            "_modified",
            "_file"
from source

)

select *
from renamed
