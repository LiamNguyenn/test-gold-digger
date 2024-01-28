{{ config(alias='deduction_category', materialized = 'view') }}
with source as (

    select * from {{ source('keypay_s3', 'deduction_category') }}

),

renamed as (

    select
        id::bigint                                     as id,  -- noqa: RF04
        deduction_category_name::varchar               as deduction_category_name,
        business_id::bigint                            as business_id,
        tax_exempt::boolean                            as tax_exempt,
        is_deleted::boolean                            as is_deleted,
        source::bigint                                 as source,  -- noqa: RF04
        external_reference_id::varchar                 as external_reference_id,
        payment_summary_classification_id::bigint      as payment_summary_classification_id,
        expense_general_ledger_mapping_code::varchar   as expense_general_ledger_mapping_code,
        liability_general_ledger_mapping_code::varchar as liability_general_ledger_mapping_code,
        sgc_calculation_impact::varchar                as sgc_calculation_impact,
        minimum_wage_deduction_impact::bigint          as minimum_wage_deduction_impact,
        is_system::boolean                             as is_system,
        deduction_category_ext_sg_id::bigint           as deduction_category_ext_sg_id,
        deduction_category_ext_uk_id::bigint           as deduction_category_ext_uk_id,
        is_resc::boolean                               as is_resc,
        is_name_read_only::boolean                     as is_name_read_only,
        is_allow_pre_tax_super::boolean                as is_allow_pre_tax_super,
        is_allow_member_voluntary::boolean             as is_allow_member_voluntary,
        _file::varchar                                 as _file,
        _transaction_date::date                        as _transaction_date,
        _etl_date::timestamp                           as _etl_date,
        _modified::timestamp                           as _modified
    from source

)

select * from renamed
