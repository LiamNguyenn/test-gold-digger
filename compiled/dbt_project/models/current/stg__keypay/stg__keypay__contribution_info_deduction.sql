
with source as (

    select * from "dev"."keypay_s3"."contribution_info_deduction"

),

renamed as (

    select
        id::varchar                  as id,  -- noqa: RF04
        contribution_info_id::bigint as contribution_info_id,
        deduction_id::bigint         as deduction_id,
        failed::boolean              as failed,
        _file::varchar               as _file,
        _transaction_date::date      as _transaction_date,
        _etl_date::timestamp         as _etl_date,
        _modified::timestamp         as _modified
    from source

)

select * from renamed