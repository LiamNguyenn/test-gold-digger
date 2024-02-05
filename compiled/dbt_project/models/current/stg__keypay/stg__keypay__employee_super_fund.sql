
with source as (

    select * from "dev"."keypay_s3"."employee_super_fund"

),

renamed as (

    select
        id::bigint                                   as id,  -- noqa: RF04
        super_fund_name::varchar                     as super_fund_name,
        member_number::varchar                       as member_number,
        allocated_percentage::double precision       as allocated_percentage,
        fixed_amount::double precision               as fixed_amount,
        employee_id::varchar                         as employee_id,
        deleted::boolean                             as deleted,
        super_fund_product_id::bigint                as super_fund_product_id,
        has_non_super_stream_compliant_fund::boolean as has_non_super_stream_compliant_fund,
        date_employee_nominated_utc::varchar         as date_employee_nominated_utc,
        super_details_default_fund_id::bigint        as super_details_default_fund_id,
        self_managed_super_fund_id::bigint           as self_managed_super_fund_id,
        _file::varchar                               as _file,
        _transaction_date::date                      as _transaction_date,
        _etl_date::timestamp                         as _etl_date,
        _modified::timestamp                         as _modified,
        allocate_balance::varchar                    as allocate_balance
    from source

)

select * from renamed