{{ config(alias='payrun_default', materialized = 'view') }}
with source as (

    select * from {{ ref('int__keypay__payrun_default') }}

),

renamed as (

    select
        id,
        employee_id,
        from_date,
        to_date,
        job_title,
        business_id,
        default_pay_category_id,
        is_payroll_tax_exempt,
        employment_agreement_id,
        default_pay_cycle_id,
        _transaction_date,
        _etl_date,
        _modified,
        _file
    from source

)

select *
from renamed
