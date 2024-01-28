{{ config(alias='payrun_total', materialized = 'view') }}
with source as (

    select * from {{ source('keypay_s3', 'payrun_total') }}

),

renamed as (

    select
        id::bigint                       as id,  -- noqa: RF04
        employee_id::bigint              as employee_id,
        payrun_id::bigint                as payrun_id,
        total_hours::float               as total_hours,
        gross_earnings::float            as gross_earnings,
        net_earnings::float              as net_earnings,
        is_excluded_from_billing::bigint as is_excluded_from_billing,
        _file::varchar                   as _file,
        _transaction_date::date          as _transaction_date,
        _etl_date::timestamp             as _etl_date,
        _modified::timestamp             as _modified
    from source

)

select * from renamed
