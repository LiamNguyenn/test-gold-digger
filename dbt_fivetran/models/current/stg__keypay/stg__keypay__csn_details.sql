{{ config(alias='csn_details', materialized = 'view') }}
with source as (

    select * from {{ source('keypay_s3', 'csn_details') }}

),

renamed as (

    select
        id::bigint                     as id,  -- noqa: RF04
        business_id::bigint            as business_id,
        cpf_submission_number::varchar as cpf_submission_number,
        csn_type::bigint               as csn_type,
        is_deleted::boolean            as is_deleted,
        _file::varchar                 as _file,
        _transaction_date::date        as _transaction_date,
        _etl_date::timestamp           as _etl_date,
        _modified::timestamp           as _modified
    from source

)

select * from renamed
