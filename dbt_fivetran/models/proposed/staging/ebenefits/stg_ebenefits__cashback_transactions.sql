with source as (
    select *

    from {{ source("ebenefits", "eh_cashback_network_txs") }}
),

transformed as (
    select
        _file::varchar              as file_name,
        _line::int                  as line_number,
        transaction_date::timestamp as transaction_timestamp,
        processed_date::timestamp   as processed_timestamp,
        campaign_name::varchar      as merchant_name,
        user_email::varchar         as user_email,
        original_amount::float      as transaction_amount,
        fee::float                  as revenue_amount,
        cashback::float             as cashback_amount,
        _fivetran_synced::timestamp as fivetran_synced_timestamp

    from source
)

select * from transformed
