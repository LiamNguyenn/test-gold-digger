
with source as (

    select * from "dev"."keypay_s3"."super_details_default_fund"

),

renamed as (

    select
        id::bigint               as id,  -- noqa: RF04
        super_details_id::bigint as super_details_id,
        usi::varchar             as usi,
        abn::varchar             as abn,
        name::varchar            as name,  -- noqa: RF04
        is_deleted::boolean      as is_deleted,
        _file::varchar           as _file,
        _transaction_date::date  as _transaction_date,
        _etl_date::timestamp     as _etl_date,
        _modified::timestamp     as _modified
    from source

)

select * from renamed