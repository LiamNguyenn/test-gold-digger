{{ config(alias='pay_event', materialized = 'view') }}
with source as (

    select * from {{ source('keypay_s3', 'pay_event') }}

),

renamed as (

    select
        id::bigint                          as id,  -- noqa: RF04
        business_id::bigint                 as business_id,
        date_created_utc::varchar           as date_created_utc,
        status::bigint                      as status,
        pay_run_id::bigint                  as pay_run_id,
        date_lodged_utc::varchar            as date_lodged_utc,
        date_response_received_utc::varchar as date_response_received_utc,
        pay_run_lodgement_data_id::bigint   as pay_run_lodgement_data_id,
        is_deleted::boolean                 as is_deleted,
        stp_version::bigint                 as stp_version,
        _file::varchar                      as _file,
        _transaction_date::date             as _transaction_date,
        _etl_date::timestamp                as _etl_date,
        _modified::timestamp                as _modified
    from source

)

select * from renamed
