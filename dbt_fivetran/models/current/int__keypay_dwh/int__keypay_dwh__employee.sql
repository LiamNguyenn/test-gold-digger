{{ config(alias="employee", materialized="incremental", unique_key="id", incremental_strategy="delete+insert", on_schema_change= "sync_all_columns") }}
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
from {{ ref("stg__keypay_dwh__employee") }}
where
    1 = 1
    {% if is_incremental() %}

        -- this filter will only be applied on an incremental run
        -- (uses > to include records whose timestamp occurred since the last run of this model)
        and _transaction_date > (select max(_transaction_date) from {{ this }})

    {% endif %}
qualify row_number()
    over (partition by id order by _transaction_date desc)
= 1
