{{ config(alias='pay_cycle', materialized = 'view') }}
with source as (

    select * from {{ source('keypay_s3', 'pay_cycle') }}

),

renamed as (

    select
        id::bigint                    as id,  -- noqa: RF04
        business_id::bigint           as business_id,
        pay_cycle_frequencyid::bigint as pay_cycle_frequencyid,
        name::varchar                 as name,  -- noqa: RF04
        last_pay_run::varchar         as last_pay_run,
        is_deleted::boolean           as is_deleted,
        aba_detailsid::bigint         as aba_detailsid,
        _file::varchar                as _file,
        _transaction_date::date       as _transaction_date,
        _etl_date::timestamp          as _etl_date,
        _modified::timestamp          as _modified
    from source

)

select * from renamed
