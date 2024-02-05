
with source as (

    select * from "dev"."keypay_s3"."system_event"

),

renamed as (

    select
        date::varchar              as date,  -- noqa: RF04
        admin_user_id::varchar     as admin_user_id,
        logged_in_user_id::varchar as logged_in_user_id,
        affected_user_id::varchar  as affected_user_id,
        business_id::varchar       as business_id,
        employee_id::varchar       as employee_id,
        white_label_id::varchar    as white_label_id,
        shard_id::varchar          as shard_id,
        _file::varchar             as _file,
        _transaction_date::date    as _transaction_date,
        _etl_date::timestamp       as _etl_date,
        _modified::timestamp       as _modified
    from source

)

select * from renamed