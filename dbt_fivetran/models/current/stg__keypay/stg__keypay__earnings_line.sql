{{ config(alias='earnings_line', materialized = 'view') }}
with source as (

    select * from {{ source('keypay_s3', 'earnings_line') }}

),

renamed as (

    select
        id::bigint                      as id,  -- noqa: RF04
        employee_id::bigint             as employee_id,
        pay_category_id::bigint         as pay_category_id,
        pay_run_id::bigint              as pay_run_id,
        units::float                    as units,
        location_id::bigint             as location_id,
        pay_run_total_id::bigint        as pay_run_total_id,
        rate::float                     as rate,
        earnings_line_status_id::bigint as earnings_line_status_id,
        external_reference_id::varchar  as external_reference_id,
        net_earnings::float             as net_earnings,
        net_earnings_reporting::float   as net_earnings_reporting,
        earnings_line_ext_au_id::bigint as earnings_line_ext_au_id,
        _file::varchar                  as _file,
        _transaction_date::date         as _transaction_date,
        _etl_date::timestamp            as _etl_date,
        _modified::timestamp            as _modified
    from source

)

select * from renamed
