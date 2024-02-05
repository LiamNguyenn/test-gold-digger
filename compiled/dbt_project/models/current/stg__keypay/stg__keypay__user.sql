
with source as (

    select * from "dev"."keypay_s3"."user"

),

renamed as (

    select
        id::bigint              as id,  -- noqa: RF04
        first_name::varchar     as first_name,
        last_name::varchar      as last_name,
        email::varchar          as email,
        is_active::boolean      as is_active,
        is_admin::boolean       as is_admin,
        _file::varchar          as _file,
        _transaction_date::date as _transaction_date,
        _etl_date::timestamp    as _etl_date,
        _modified::timestamp    as _modified
    from source

)

select * from renamed