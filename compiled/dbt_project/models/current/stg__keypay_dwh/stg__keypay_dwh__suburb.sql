
with source as (

    select * from "dev"."keypay_s3"."suburb"

),

renamed as (

    select
        id::bigint              as id,  -- noqa: RF04
        name::varchar           as name,  -- noqa: RF04
        postcode::varchar       as postcode,
        state::varchar          as state,  -- noqa: RF04
        country::varchar        as country,
        _file::varchar          as _file,
        _transaction_date::date as _transaction_date,
        _etl_date::timestamp    as _etl_date,
        _modified::timestamp    as _modified
    from source

)

select * from renamed