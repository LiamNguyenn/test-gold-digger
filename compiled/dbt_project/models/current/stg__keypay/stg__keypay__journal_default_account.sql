
with source as (

    select * from "dev"."keypay_s3"."journal_default_account"

),

renamed as (

    select
        id::bigint              as id,  -- noqa: RF04
        business_id::bigint     as business_id,
        account_type::bigint    as account_type,
        _file::varchar          as _file,
        _transaction_date::date as _transaction_date,
        _etl_date::timestamp    as _etl_date,
        _modified::timestamp    as _modified
    from source

)

select * from renamed