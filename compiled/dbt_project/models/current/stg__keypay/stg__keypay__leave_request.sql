
with source as (

    select * from "dev"."keypay_s3"."leave_request"

),

renamed as (

    select
        id::bigint                as id,  -- noqa: RF04
        employee_id::bigint       as employee_id,
        from_date::varchar        as from_date,
        to_date::varchar          as to_date,
        total_hours::float        as total_hours,
        requested_date::varchar   as requested_date,
        status::varchar           as status,
        business_id::bigint       as business_id,
        leave_category_id::bigint as leave_category_id,
        _file::varchar            as _file,
        _transaction_date::date   as _transaction_date,
        _etl_date::timestamp      as _etl_date,
        _modified::timestamp      as _modified
    from source

)

select * from renamed