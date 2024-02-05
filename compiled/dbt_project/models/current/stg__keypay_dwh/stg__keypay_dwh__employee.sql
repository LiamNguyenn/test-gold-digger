
with source as (

    select * from "dev"."keypay_s3"."employee"

),

renamed as (

    select
        id::bigint                          as id,  -- noqa: RF04
        business_id::bigint                 as business_id,
        firstname::varchar                  as firstname,
        surname::varchar                    as surname,
        date_created::date                  as date_created,
        date_of_birth::date                 as date_of_birth,
        residential_street_address::varchar as residential_street_address,
        residential_suburb_id::varchar      as residential_suburb_id,
        start_date::date                    as start_date,
        end_date::date                      as end_date,
        gender::varchar                     as gender,
        payrollid::varchar                  as payrollid,
        pay_run_default_id::varchar         as pay_run_default_id,
        tax_file_declaration_id::varchar    as tax_file_declaration_id,
        email::varchar                      as email,
        home_phone::varchar                 as home_phone,
        work_phone::varchar                 as work_phone,
        mobile_phone::varchar               as mobile_phone,
        employee_onboarding_id::bigint      as employee_onboarding_id,
        status::varchar                     as status,
        _file::varchar                      as _file,
        _transaction_date::date             as _transaction_date,
        _etl_date::timestamp                as _etl_date,
        _modified::timestamp                as _modified
    from source

)

select * from renamed