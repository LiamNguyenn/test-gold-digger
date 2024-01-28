{{ config(alias='white_label', materialized = 'view') }}
    with source as (

select * from {{ ref('int__keypay__white_label') }}

),

renamed as (

select
            "id",
            "name",
            "is_deleted",
            "region_id",
            "support_email",
            "primary_champion_id",
            "function_enable_super_choice_marketplace",
            "default_billing_plan_id",
            "reseller_id",
            "_file",
            "_transaction_date",
            "_etl_date",
            "_modified"
from source

)

select *
from renamed
