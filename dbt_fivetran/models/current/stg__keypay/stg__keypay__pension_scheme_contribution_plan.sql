{{ config(alias='pension_scheme_contribution_plan', materialized = 'view') }}
with source as (

    select * from {{ source('keypay_s3', 'pension_scheme_contribution_plan') }}

),

renamed as (

    select
        id::bigint                                         as id,  -- noqa: RF04
        employee_contribution_percentage::double precision as employee_contribution_percentage,
        employer_contribution_percentage::double precision as employer_contribution_percentage,
        pension_type::bigint                               as pension_type,
        max_earnings_threshold::double precision           as max_earnings_threshold,
        min_earnings_threshold::double precision           as min_earnings_threshold,
        contribution_group_name::varchar                   as contribution_group_name,
        contribution_group_id::varchar                     as contribution_group_id,
        contribution_plan_name::varchar                    as contribution_plan_name,
        reporting_frequency::bigint                        as reporting_frequency,
        calculate_on_qualifying_earnings::boolean          as calculate_on_qualifying_earnings,
        pension_scheme_id::bigint                          as pension_scheme_id,
        contribution_plan_id::bigint                       as contribution_plan_id,
        collection_source_id::varchar                      as collection_source_id,
        is_deleted::boolean                                as is_deleted,
        salary_sacrifice_deduction_category_id::bigint     as salary_sacrifice_deduction_category_id,
        salary_sacrifice_pay_category_ids                  as salary_sacrifice_pay_category_ids,
        employee_contribution_pay_category_ids             as employee_contribution_pay_category_ids,
        employer_contribution_pay_category_ids             as employer_contribution_pay_category_ids,
        decode(
            lower(is_auto_enrolment_scheme),
            'true', TRUE,
            'false', FALSE
        )::boolean                                         as is_auto_enrolment_scheme,
        lower_earnings_disregard::double precision         as lower_earnings_disregard,
        lower_default_earnings_disregard_type::bigint      as lower_default_earnings_disregard_type,
        upper_earnings_cap::double precision               as upper_earnings_cap,
        upper_default_earnings_cap_type::bigint            as upper_default_earnings_cap_type,
        use_tax_month_pay_period::boolean                  as use_tax_month_pay_period,
        salary_sacrifice_percentage::double precision      as salary_sacrifice_percentage,
        nic_saving_rebate_percentage::varchar              as nic_saving_rebate_percentage,
        _file::varchar                                     as _file,
        _transaction_date::date                            as _transaction_date,
        _etl_date::timestamp                               as _etl_date,
        _modified::timestamp                               as _modified
    from source

)

select * from renamed
