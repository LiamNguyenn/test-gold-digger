{{ config(alias='leave_accrual', materialized = 'view') }}
with source as (

    select * from {{ source('keypay_s3', 'leave_accrual') }}

),

renamed as (

    select
        id::bigint                as id,  -- noqa: RF04
        employee_id::bigint       as employee_id,
        accrued_amount::float     as accrued_amount,
        accrual_status_id::bigint as accrual_status_id,
        _file::varchar            as _file,
        _transaction_date::date   as _transaction_date,
        _etl_date::timestamp      as _etl_date,
        _modified::timestamp      as _modified
    from source

)

select * from renamed
