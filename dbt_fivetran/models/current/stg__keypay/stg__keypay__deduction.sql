{{ config(alias='deduction', materialized = 'view') }}
with source as (

    select * from {{ source('keypay_s3', 'deduction') }}

),

renamed as (

    select
        id::bigint                                               as id,  -- noqa: RF04
        employee_id::bigint                                      as employee_id,
        pay_run_total_id::bigint                                 as pay_run_total_id,
        deduction_category_id::bigint                            as deduction_category_id,
        amount::float                                            as amount,
        pay_run_id::bigint                                       as pay_run_id,
        employee_super_fund_id::bigint                           as employee_super_fund_id,
        contribution_info_id::varchar                            as contribution_info_id,
        associated_employee_deduction_category_id::bigint        as associated_employee_deduction_category_id,
        bank_account_id::bigint                                  as bank_account_id,
        is_resc::boolean                                         as is_resc,
        bank_account_bsb::varchar                                as bank_account_bsb,
        bank_account_number::varchar                             as bank_account_number,
        bank_account_type::bigint                                as bank_account_type,
        is_member_voluntary::boolean                             as is_member_voluntary,
        associated_employee_pension_contribution_plan_id::bigint as associated_employee_pension_contribution_plan_id,
        is_pension_scheme_salary_sacrifice::boolean              as is_pension_scheme_salary_sacrifice,
        additional_data::bigint                                  as additional_data,
        decode(
            lower(paid_to_tax_office),
            'true', TRUE,
            'false', FALSE
        )::boolean                                               as paid_to_tax_office,
        payg_adjustment_id::bigint                               as payg_adjustment_id,
        bank_account_swift::varchar                              as bank_account_swift,
        bank_account_branch_code::varchar                        as bank_account_branch_code,
        _file::varchar                                           as _file,
        _transaction_date::date                                  as _transaction_date,
        _etl_date::timestamp                                     as _etl_date,
        _modified::timestamp                                     as _modified
    from source

)

select * from renamed
