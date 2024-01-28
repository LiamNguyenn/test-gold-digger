{{ config(alias='user_whitelabel', materialized = 'view') }}
with source as (

    select * from {{ source('keypay_s3', 'user_whitelabel') }}

),

renamed as (

    select
        user_id::bigint            as user_id,
        whitelabel_id::bigint      as whitelabel_id,
        is_default_parent::varchar as is_default_parent,
        _file::varchar             as _file,
        _transaction_date::date    as _transaction_date,
        _etl_date::timestamp       as _etl_date,
        _modified::timestamp       as _modified,
        userid::varchar            as userid
    from source

)

select * from renamed
