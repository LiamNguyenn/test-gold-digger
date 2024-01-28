{{ config(alias='statutory_settings', materialized = 'view') }}
with source as (

    select * from {{ source('keypay_s3', 'statutory_settings') }}

),

renamed as (

    select
        id::bigint                            as id,  -- noqa: RF04
        business_id::bigint                   as business_id,
        income_tax_number__encrypted::varchar as income_tax_number_encrypted,
        e_number::varchar                     as e_number,
        epf_number::varchar                   as epf_number,
        socso_number::varchar                 as socso_number,
        hrdf_status::varchar                  as hrdf_status,
        _file::varchar                        as _file,
        _transaction_date::date               as _transaction_date,
        _etl_date::timestamp                  as _etl_date,
        _modified::timestamp                  as _modified
    from source

)

select * from renamed
