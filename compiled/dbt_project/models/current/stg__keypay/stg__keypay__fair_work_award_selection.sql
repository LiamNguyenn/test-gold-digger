
with source as (

    select * from "dev"."keypay_s3"."fair_work_award_selection"

),

renamed as (

    select
        id::bigint                 as id,  -- noqa: RF04
        fair_work_award_id::bigint as fair_work_award_id,
        business_id::bigint        as business_id,
        date_time_utc::varchar     as date_time_utc,
        source::bigint             as source,  -- noqa: RF04
        _file::varchar             as _file,
        _transaction_date::date    as _transaction_date,
        _etl_date::timestamp       as _etl_date,
        _modified::timestamp       as _modified
    from source

)

select * from renamed