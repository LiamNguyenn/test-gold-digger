{{ config(alias='earnings_line_ext_au', materialized = 'view') }}
with source as (

    select * from {{ source('keypay_s3', 'earnings_line_ext_au') }}

),

renamed as (

    select
        id::bigint                                    as id,  -- noqa: RF04
        earnings_line_id::bigint                      as earnings_line_id,
        pay_run_id::bigint                            as pay_run_id,
        average_total_earnings_amount::float          as average_total_earnings_amount,
        average_total_earnings_payg_amount::float     as average_total_earnings_payg_amount,
        average_additional_payments_amount::float     as average_additional_payments_amount,
        full_earnings_payg_amount::float              as full_earnings_payg_amount,
        calculated_payg_amount::float                 as calculated_payg_amount,
        max_payg_amount::float                        as max_payg_amount,
        gross_earnings_amount::float                  as gross_earnings_amount,
        gross_earnings_payg_amount::float             as gross_earnings_payg_amount,
        pre_adjustment_payg_withholding_amount::float as pre_adjustment_payg_withholding_amount,
        gross_earnings_stsl_amount::float             as gross_earnings_stsl_amount,
        average_total_earnings_stsl_amount::float     as average_total_earnings_stsl_amount,
        full_earnings_stsl_amount::float              as full_earnings_stsl_amount,
        calculated_stsl_amount::float                 as calculated_stsl_amount,
        lump_sum_e_financial_year::bigint             as lump_sum_e_financial_year,
        _file::varchar                                as _file,
        _transaction_date::date                       as _transaction_date,
        _etl_date::timestamp                          as _etl_date,
        _modified::timestamp                          as _modified
    from source

)

select * from renamed
