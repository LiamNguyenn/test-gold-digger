{{ config(alias='bank_payment_file_details', materialized = 'view') }}
with source as (

    select * from {{ source('keypay_s3', 'bank_payment_file_details') }}

),

renamed as (

    select
        id::varchar                              as id,  -- noqa: RF04
        business_id::varchar                     as business_id,
        file_format::varchar                     as file_format,
        originating_account_number::varchar      as originating_account_number,
        originating_account_name::varchar        as originating_account_name,
        lodgement_reference::varchar             as lodgement_reference,
        merge_multiple_account_payments::boolean as merge_multiple_account_payments,
        payment_additional_content::varchar      as payment_additional_content,
        transaction_reference_number::varchar    as transaction_reference_number,
        is_confidential::boolean                 as is_confidential,
        is_payment_integration::boolean          as is_payment_integration,
        _file::varchar                           as _file,
        _transaction_date::date                  as _transaction_date,
        _etl_date::timestamp                     as _etl_date,
        _modified::timestamp                     as _modified
    from source

)

select * from renamed
