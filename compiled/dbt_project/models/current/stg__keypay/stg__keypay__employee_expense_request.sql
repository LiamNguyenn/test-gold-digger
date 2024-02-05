
with source as (

    select * from "dev"."keypay_s3"."employee_expense_request"

),

renamed as (

    select
        id::bigint                        as id,  -- noqa: RF04
        employee_id::bigint               as employee_id,
        status::bigint                    as status,
        created_by_user_id::bigint        as created_by_user_id,
        date_created_utc::varchar         as date_created_utc,
        status_updated_by_user_id::bigint as status_updated_by_user_id,
        date_status_updated_utc::varchar  as date_status_updated_utc,
        description::varchar              as description,
        status_update_notes::varchar      as status_update_notes,
        pay_run_total_id::bigint          as pay_run_total_id,
        business_id::bigint               as business_id,
        date_first_approved_utc::varchar  as date_first_approved_utc,
        _file::varchar                    as _file,
        _transaction_date::date           as _transaction_date,
        _etl_date::timestamp              as _etl_date,
        _modified::timestamp              as _modified
    from source

)

select * from renamed