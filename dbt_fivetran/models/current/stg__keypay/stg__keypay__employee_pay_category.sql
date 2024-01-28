{{ config(alias='employee_pay_category', materialized = 'view') }}
with source as (

    select * from {{ source('keypay_s3', 'employee_pay_category') }}

),

renamed as (

    select
        id::bigint                         as id,  -- noqa: RF04
        calculated_rate::float             as calculated_rate,
        employee_id::bigint                as employee_id,
        standard_weekly_hours::float       as standard_weekly_hours,
        is_default::boolean                as is_default,
        from_date::varchar                 as from_date,
        to_date::varchar                   as to_date,
        user_supplied_rate::float          as user_supplied_rate,
        standard_daily_hours::varchar      as standard_daily_hours,
        pay_category_rate_unit_id::varchar as pay_category_rate_unit_id,
        employee_rate_unit_id::varchar     as employee_rate_unit_id,
        expiry_date::varchar               as expiry_date,
        _file::varchar                     as _file,
        _transaction_date::date            as _transaction_date,
        _etl_date::timestamp               as _etl_date,
        _modified::timestamp               as _modified,
        pay_category_id::varchar           as pay_category_id
    from source

)

select * from renamed
