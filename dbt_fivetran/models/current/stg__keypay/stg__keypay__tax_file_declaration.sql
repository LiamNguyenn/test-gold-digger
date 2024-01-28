{{ config(alias='tax_file_declaration', materialized = 'view') }}
with source as (

    select * from {{ source('keypay_s3', 'tax_file_declaration') }}

),

renamed as (

    select
        id::bigint                  as id,  -- noqa: RF04
        employee_id::varchar        as employee_id,
        employment_type_id::varchar as employment_type_id,
        from_date::varchar          as from_date,
        to_date::varchar            as to_date,
        _file::varchar              as _file,
        _transaction_date::date     as _transaction_date,
        _etl_date::timestamp        as _etl_date,
        _modified::timestamp        as _modified
    from source

)

select * from renamed
