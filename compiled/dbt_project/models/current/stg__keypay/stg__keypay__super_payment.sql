
with source as (

    select * from "dev"."keypay_s3"."super_payment"

),

renamed as (

    select
        id::bigint                     as id,  -- noqa: RF04
        pay_run_total_id::bigint       as pay_run_total_id,
        employee_super_fund_id::bigint as employee_super_fund_id,
        amount::float                  as amount,
        pay_run_id::bigint             as pay_run_id,
        employee_id::bigint            as employee_id,
        contribution_type::bigint      as contribution_type,
        _file::varchar                 as _file,
        _transaction_date::date        as _transaction_date,
        _etl_date::timestamp           as _etl_date,
        _modified::timestamp           as _modified
    from source

)

select * from renamed