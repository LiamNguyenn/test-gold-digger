{{ config(alias='user_employee_group', materialized = 'view') }}
    with source as (

select * from {{ ref('int__keypay__user_employee_group') }}

),

renamed as (

select
            "id",
            "user_id",
            "employee_group_id",
            "permissions",
            "_transaction_date",
            "_etl_date",
            "_modified",
            "_file"
from source

)

select *
from renamed
