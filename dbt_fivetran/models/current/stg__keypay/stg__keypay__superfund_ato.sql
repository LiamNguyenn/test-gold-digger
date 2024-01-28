{{ config(alias='superfund_ato', materialized = 'view') }}
with source as (

    select * from {{ source('keypay_s3', 'superfund_ato') }}

),

renamed as (

    select
        regexp_replace(abn, '([^ \\t])[ \\t]+$', '\\1')::varchar                       as abn,
        regexp_replace(fund_name, '([^ \\t])[ \\t]+$', '\\1')::varchar                 as fund_name,
        regexp_replace(usi, '([^ \\t])[ \\t]+$', '\\1')::varchar                       as usi,
        regexp_replace(product_name, '([^ \\t])[ \\t]+$', '\\1')::varchar              as product_name,
        regexp_replace(contribution_restrictions, '([^ \\t])[ \\t]+$', '\\1')::varchar as contribution_restrictions,
        from_date::varchar                                                             as from_date,
        to_date::varchar                                                               as to_date,
        _transaction_date::timestamp                                                   as _transaction_date,
        _etl_date::timestamp                                                           as _etl_date
    from source

)

select * from renamed
