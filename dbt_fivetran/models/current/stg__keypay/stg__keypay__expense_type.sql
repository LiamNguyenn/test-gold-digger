{{ config(alias='expense_type', materialized = 'view') }}
with source as (

    select * from {{ source('keypay_s3', 'expense_type') }}

),

renamed as (

    select
        id::bigint              as id,  -- noqa: RF04
        description::varchar    as description,
        unit_cost::float        as unit_cost,
        _file::varchar          as _file,
        _transaction_date::date as _transaction_date,
        _etl_date::timestamp    as _etl_date,
        _modified::timestamp    as _modified
    from source

)

select * from renamed
