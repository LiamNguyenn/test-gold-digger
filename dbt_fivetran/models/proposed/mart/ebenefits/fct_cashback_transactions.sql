select
    transaction_timestamp,
    processed_timestamp,
    {{ get_date_id("transaction_timestamp") }} as dim_date_sk,
    merchant_name,
    regexp_substr(user_email, '[0-9A-fa-f]{8}(-[0-9A-fa-f]{4}){3}-[0-9A-fa-f]{12}') as dim_user_ebenefits_user_uuid,
    transaction_amount,
    revenue_amount,
    cashback_amount

from {{ ref("stg_ebenefits__cashback_transactions") }}
