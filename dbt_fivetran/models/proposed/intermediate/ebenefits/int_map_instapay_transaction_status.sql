select distinct
    transaction_status_key,
    case
        when transaction_status_key = 0 then 'pending'
        when transaction_status_key = 1 then 'payment_processed'
        when transaction_status_key = 2 then 'complete'
        when transaction_status_key = 3 then 'error'
        when transaction_status_key = 4 then 'draft'
        when transaction_status_key = 5 then 'revert'
        when transaction_status_key = 100 then 'shaype_processing'
        when transaction_status_key = 101 then 'shaype_accepted'
        when transaction_status_key = 102 then 'shaype_refused'
        else 'unknown'
    end as transaction_status

from {{ ref("stg_heropay_db__heropay_transactions") }}
