{{ config(alias='leave_category', materialized = 'view') }}
    with source as (

select * from {{ ref('int__keypay__leave_category') }}

),

renamed as (

select
            "id",
            "leave_category_name",
            "business_id",
            "exclude_from_termination_payout",
            "is_deleted",
            "unit_type",
            "source",
            "date_created",
            "deduct_from_primary_pay_category",
            "deduct_from_pay_category_id",
            "transfer_to_pay_category_id",
            "leave_category_type",
            "entitlement_period",
            "contingent_period",
            "automatically_accrues",
            "standard_hours_per_year",
            "units",
            "_transaction_date",
            "_etl_date",
            "_modified",
            "_file"
from source

)

select *
from renamed
