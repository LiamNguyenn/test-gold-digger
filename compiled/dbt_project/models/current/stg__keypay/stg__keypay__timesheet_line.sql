
with source as (

    select * from "dev"."keypay_s3"."timesheet_line"

),

renamed as (

    select
        id::bigint                                  as id,  -- noqa: RF04
        employee_id::bigint                         as employee_id,
        start_time::varchar                         as start_time,
        end_time::varchar                           as end_time,
        units::float                                as units,
        date_created::varchar                       as date_created,
        submitted_start_time::varchar               as submitted_start_time,
        submitted_end_time::varchar                 as submitted_end_time,
        pay_category_id::bigint                     as pay_category_id,
        status::bigint                              as status,
        leave_request_id::bigint                    as leave_request_id,
        consolidated_with_timesheet_line_id::bigint as consolidated_with_timesheet_line_id,
        pay_run_total_id::bigint                    as pay_run_total_id,
        business_id::bigint                         as business_id,
        auto_approved_by_roster_shift_id::bigint    as auto_approved_by_roster_shift_id,
        _file::varchar                              as _file,
        _transaction_date::date                     as _transaction_date,
        _etl_date::timestamp                        as _etl_date,
        _modified::timestamp                        as _modified
    from source

)

select * from renamed