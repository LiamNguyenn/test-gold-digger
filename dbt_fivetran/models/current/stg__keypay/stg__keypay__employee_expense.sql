{{ config(alias='employee_expense', materialized = 'view') }}
with source as (

    select * from {{ source('keypay_s3', 'employee_expense') }}

),

renamed as (

    select
        id::bigint                            as id,  -- noqa: RF04
        pay_run_id::bigint                    as pay_run_id,
        pay_run_total_id::bigint              as pay_run_total_id,
        employee_id::bigint                   as employee_id,
        employee_expense_category_id::bigint  as employee_expense_category_id,
        location_id::bigint                   as location_id,
        business_id::bigint                   as business_id,
        amount::float                         as amount,
        notes::varchar                        as notes,
        external_id::varchar                  as external_id,
        employee_recurring_expense_id::bigint as employee_recurring_expense_id,
        employee_expense_request_id::bigint   as employee_expense_request_id,
        tax_code::varchar                     as tax_code,
        tax_rate::double precision            as tax_rate,
        tax_code_display_name::varchar        as tax_code_display_name,
        _file::varchar                        as _file,
        _transaction_date::date               as _transaction_date,
        _etl_date::timestamp                  as _etl_date,
        _modified::timestamp                  as _modified
    from source

)

select * from renamed
