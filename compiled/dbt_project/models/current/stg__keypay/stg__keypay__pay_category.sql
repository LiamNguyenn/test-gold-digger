
with source as (

    select * from "dev"."keypay_s3"."pay_category"

),

renamed as (

    select
        id::bigint                                as id,  -- noqa: RF04
        pay_category_name::varchar                as pay_category_name,
        business_id::bigint                       as business_id,
        date_created::varchar                     as date_created,
        rate_unit_id::bigint                      as rate_unit_id,
        default_super_rate::double precision      as default_super_rate,
        is_tax_exempt::boolean                    as is_tax_exempt,
        linked_pay_category_id::bigint            as linked_pay_category_id,
        is_deleted::boolean                       as is_deleted,
        external_reference_id::varchar            as external_reference_id,
        source::bigint                            as source,  -- noqa: RF04
        is_payroll_tax_exempt::boolean            as is_payroll_tax_exempt,
        pay_category_type::bigint                 as pay_category_type,
        business_award_package_id::bigint         as business_award_package_id,
        payment_summary_classification_id::bigint as payment_summary_classification_id,
        general_ledger_mapping_code::varchar      as general_ledger_mapping_code,
        super_liability_mapping_code::varchar     as super_liability_mapping_code,
        super_expense_mapping_code::varchar       as super_expense_mapping_code,
        is_w1_exempt::boolean                     as is_w1_exempt,
        number_of_decimal_places::bigint          as number_of_decimal_places,
        is_national_insurance_exempt::boolean     as is_national_insurance_exempt,
        minimum_wage_calculation_impact::bigint   as minimum_wage_calculation_impact,
        decode(
            lower(exclude_from_average_earnings),
            'true', TRUE,
            'false', FALSE
        )::boolean                                as exclude_from_average_earnings,
        cpf_classification_id::bigint             as cpf_classification_id,
        include_in_gross_rate_of_pay::boolean     as include_in_gross_rate_of_pay,
        exclude_from_ordinary_earnings::boolean   as exclude_from_ordinary_earnings,
        hide_units_on_pay_slip::boolean           as hide_units_on_pay_slip,
        pay_category_ext_my_id::bigint            as pay_category_ext_my_id,
        rounding_method::bigint                   as rounding_method,
        pay_category_ext_nz_id::bigint            as pay_category_ext_nz_id,
        pay_category_ext_uk_id::bigint            as pay_category_ext_uk_id,
        allowance_description::varchar            as allowance_description,
        pay_category_ext_sg_id::bigint            as pay_category_ext_sg_id,
        accrues_leave::varchar                    as accrues_leave,
        penalty_loading_multiplier::varchar       as penalty_loading_multiplier,
        rate_loading_multiplier::varchar          as rate_loading_multiplier,
        _file::varchar                            as _file,
        _transaction_date::date                   as _transaction_date,
        _etl_date::timestamp                      as _etl_date,
        _modified::timestamp                      as _modified
    from source

)

select * from renamed