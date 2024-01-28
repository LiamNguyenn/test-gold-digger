select distinct
    transaction_type_key,
    case
        when transaction_type_key = 0 then 'topup'
        when transaction_type_key = 1 then 'withdrawal'
        when transaction_type_key = 2 then 'topup_reversion'
        when transaction_type_key = 3 then 'withdrawal_reversion'
        when transaction_type_key = 4 then 'deduction'
        when transaction_type_key = 5 then 'deduction_reversion'
        else 'unknown'
    end as transaction_type

from {{ ref("stg_herodollar_service_public__herodollar_transactions") }}
