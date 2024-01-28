{{ config(alias='region', materialized = 'view') }}
with source as (

    select * from {{ source('keypay_s3', 'region') }}

),

renamed as (

    select
        id::varchar                       as id,  -- noqa: RF04
        currency::varchar                 as currency,
        name::varchar                     as name,  -- noqa: RF04
        culturename::varchar              as culture_name,
        defaultstandardhoursperday::float as default_standard_hours_per_day,
        commencebillingfrom::varchar      as commence_billing_from,
        minimumbillableamount::float      as minimum_bill_able_amount,
        _file::varchar                    as _file,
        _transaction_date::date           as _transaction_date,
        _etl_date::timestamp              as _etl_date,
        _modified::timestamp              as _modified
    from source

)

select * from renamed
