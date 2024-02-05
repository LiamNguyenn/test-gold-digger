
with source as (

    select * from "dev"."keypay_s3"."award_package"

),

renamed as (

    select
        id::bigint                  as id,  -- noqa: RF04
        name::varchar               as name,  -- noqa: RF04
        date_created_utc::varchar   as date_created_utc,
        fair_work_award_id::varchar as fair_work_award_id,
        is_disabled::boolean        as is_disabled,
        _file::varchar              as _file,
        _transaction_date::date     as _transaction_date,
        _etl_date::timestamp        as _etl_date,
        _modified::timestamp        as _modified
    from source

)

select * from renamed