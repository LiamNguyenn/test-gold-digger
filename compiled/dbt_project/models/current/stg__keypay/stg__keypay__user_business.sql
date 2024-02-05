
with source as (

    select * from "dev"."keypay_s3"."user_business"

),

renamed as (

    select
        user_id::bigint                    as user_id,
        business_id::bigint                as business_id,
        is_single_sign_on_enabled::varchar as is_single_sign_on_enabled,
        _file::varchar                     as _file,
        _transaction_date::date            as _transaction_date,
        _etl_date::timestamp               as _etl_date,
        _modified::timestamp               as _modified
    from source

)

select * from renamed