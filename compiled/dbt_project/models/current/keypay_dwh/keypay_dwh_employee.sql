
with source as (

    select * from "dev"."int__keypay_dwh"."employee"

),

renamed as (

    select
        id,
        business_id,
        firstname,
        surname,
        date_created,
        date_of_birth,
        residential_street_address,
        residential_suburb_id,
        start_date,
        end_date,
        gender,
        payrollid,
        pay_run_default_id,
        tax_file_declaration_id,
        email,
        home_phone,
        work_phone,
        mobile_phone,
        employee_onboarding_id,
        status,
        _transaction_date,
        _etl_date,
        _modified,
        _file
    from source

)

select *
from renamed