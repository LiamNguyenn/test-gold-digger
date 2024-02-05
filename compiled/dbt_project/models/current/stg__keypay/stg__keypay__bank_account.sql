
with source as (

    select * from "dev"."keypay_s3"."bank_account"

),

renamed as (

    select
        id::bigint                     as id,  -- noqa: RF04
        employee_id::varchar           as employee_id,
        external_reference_id::varchar as external_reference_id,
        source::varchar                as source,  -- noqa: RF04
        account_type::varchar          as account_type,
        _file::varchar                 as _file,
        _transaction_date::date        as _transaction_date,
        _etl_date::timestamp           as _etl_date,
        _modified::timestamp           as _modified
    from source

)

select * from renamed