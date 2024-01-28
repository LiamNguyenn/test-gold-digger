{{ config(alias='employee_pay_category', materialized = 'view') }}
with source as (

    select * from {{ ref('int__keypay__employee_pay_category') }}

),

renamed as (

    select
        id,
        calculated_rate,
        employee_id,
        standard_weekly_hours,
        is_default,
        from_date,
        to_date,
        user_supplied_rate,
        standard_daily_hours,
        pay_category_rate_unit_id,
        employee_rate_unit_id,
        expiry_date,
        pay_category_id,
        _transaction_date,
        _etl_date,
        _modified,
        _file
    from source

)

select *
from renamed
