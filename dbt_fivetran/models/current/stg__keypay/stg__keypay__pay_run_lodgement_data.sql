{{ config(alias='pay_run_lodgement_data', materialized = 'view') }}
with source as (

    select * from {{ source('keypay_s3', 'pay_run_lodgement_data') }}

),

renamed as (

    select
        id::bigint              as id,  -- noqa: RF04
        status::bigint          as status,
        is_test::boolean        as is_test,
        _file::varchar          as _file,
        _transaction_date::date as _transaction_date,
        _etl_date::timestamp    as _etl_date,
        _modified::timestamp    as _modified
    from source

)

select * from renamed
