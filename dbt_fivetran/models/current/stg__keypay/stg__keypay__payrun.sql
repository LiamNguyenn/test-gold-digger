{{ config(alias='payrun', materialized = 'view') }}
with source as (

    select * from {{ source('keypay_s3', 'payrun') }}

),

renamed as (

    select
        id::bigint                        as id,  -- noqa: RF04
        date_finalised::varchar           as date_finalised,
        pay_period_starting::varchar      as pay_period_starting,
        pay_period_ending::varchar        as pay_period_ending,
        date_paid::varchar                as date_paid,
        business_id::bigint               as business_id,
        invoice_id::bigint                as invoice_id,
        date_first_finalised::date        as date_first_finalised,
        pay_run_lodgement_data_id::bigint as pay_run_lodgement_data_id,
        notification_date::varchar        as notification_date,
        finalised_by_id::bigint           as finalised_by_id,
        pay_cycle_id::bigint              as pay_cycle_id,
        pay_cycle_frequency_id::bigint    as pay_cycle_frequency_id,
        date_created_utc::varchar         as date_created_utc,
        _file::varchar                    as _file,
        _transaction_date::date           as _transaction_date,
        _etl_date::timestamp              as _etl_date,
        _modified::timestamp              as _modified
    from source

)

select * from renamed
