
with source as (

    select * from "dev"."keypay_s3"."contribution_info"

),

renamed as (

    select
        id::bigint              as id,  -- noqa: RF04
        cont_amount::float      as cont_amount,
        cont_type::varchar      as cont_type,
        super_member_id::bigint as super_member_id,
        employee_id::bigint     as employee_id,
        failed::boolean         as failed,
        _file::varchar          as _file,
        _transaction_date::date as _transaction_date,
        _etl_date::timestamp    as _etl_date,
        _modified::timestamp    as _modified
    from source

)

select * from renamed