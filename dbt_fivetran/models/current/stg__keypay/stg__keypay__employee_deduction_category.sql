{{ config(alias='employee_deduction_category', materialized = 'view') }}
with source as (

    select * from {{ source('keypay_s3', 'employee_deduction_category') }}

),

renamed as (

    select
        id::bigint                                           as id,  -- noqa: RF04
        employee_id::bigint                                  as employee_id,
        deduction_category_id::bigint                        as deduction_category_id,
        from_date::varchar                                   as from_date,
        to_date::varchar                                     as to_date,
        amount::float                                        as amount,
        employee_super_fund_id::bigint                       as employee_super_fund_id,
        expiry_date::varchar                                 as expiry_date,
        maximum_amount_paid::double precision                as maximum_amount_paid,
        is_active::boolean                                   as is_active,
        bank_account_id::bigint                              as bank_account_id,
        deleted::boolean                                     as deleted,
        notes::varchar                                       as notes,
        external_reference_id::varchar                       as external_reference_id,
        source::bigint                                       as source,  -- noqa: RF04
        deduction_type::bigint                               as deduction_type,
        preserved_earnings::bigint                           as preserved_earnings,
        preserved_earnings_amount::float                     as preserved_earnings_amount,
        preserved_earnings_amount_not_reached_action::bigint as preserved_earnings_amount_not_reached_action,
        carry_forward_unpaid_deductions::boolean             as carry_forward_unpaid_deductions,
        payment_reference::varchar                           as payment_reference,
        employee_pension_contribution_plan_id::bigint        as employee_pension_contribution_plan_id,
        additional_data::bigint                              as additional_data,
        decode(
            lower(paid_to_tax_office),
            'true', TRUE,
            'false', FALSE
        )::boolean                                           as paid_to_tax_office,
        priority::bigint                                     as priority,
        student_loan_deduction_option::bigint                as student_loan_deduction_option,
        decode(
            lower(carry_forward_unused_preserved_earnings),
            'true', TRUE,
            'false', FALSE
        )::boolean                                           as carry_forward_unused_preserved_earnings,
        tiered_deduction_settings_id::bigint                 as tiered_deduction_settings_id,
        paid_to_external_service::bigint                     as paid_to_external_service,
        employee_deduction_category_ext_sg_id::bigint        as employee_deduction_category_ext_sg_id,
        employee_deduction_category_ext_my_id::bigint        as employee_deduction_category_ext_my_id,
        _file::varchar                                       as _file,
        _transaction_date::date                              as _transaction_date,
        _etl_date::timestamp                                 as _etl_date,
        _modified::timestamp                                 as _modified
    from source

)

select * from renamed
