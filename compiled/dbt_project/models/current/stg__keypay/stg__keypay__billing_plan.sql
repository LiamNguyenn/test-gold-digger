
with source as (

    select * from "dev"."keypay_s3"."billing_plan"

),

renamed as (

    select
        id::bigint                           as id,  -- noqa: RF04
        name::varchar                        as name,  -- noqa: RF04
        function_employee_onboarding::bigint as function_employee_onboarding,
        price_per_unit::float                as price_per_unit,
        _file::varchar                       as _file,
        _transaction_date::date              as _transaction_date,
        _etl_date::timestamp                 as _etl_date,
        _modified::timestamp                 as _modified
    from source

)

select * from renamed