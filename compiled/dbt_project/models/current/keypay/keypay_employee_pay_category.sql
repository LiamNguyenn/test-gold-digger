
with source as (

    select * from "dev"."int__keypay"."employee_pay_category"

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