{{ config(alias='payrun_default', materialized = 'view') }}
with source as (

    select * from {{ source('keypay_s3', 'payrun_default') }}

),

renamed as (

    select
        id::bigint                       as id,  -- noqa: RF04
        employee_id::bigint              as employee_id,
        from_date::varchar               as from_date,
        to_date::varchar                 as to_date,
        job_title::varchar               as job_title,
        business_id::varchar             as business_id,
        default_pay_category_id::bigint  as default_pay_category_id,
        is_payroll_tax_exempt::boolean   as is_payroll_tax_exempt,
        employment_agreement_id::varchar as employment_agreement_id,
        _file::varchar                   as _file,
        _transaction_date::date          as _transaction_date,
        _etl_date::timestamp             as _etl_date,
        _modified::timestamp             as _modified,
        default_pay_cycle_id::varchar    as default_pay_cycle_id
    from source

)

select * from renamed
