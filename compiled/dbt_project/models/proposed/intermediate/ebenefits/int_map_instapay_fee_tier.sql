select distinct
    fee_tier_key,
    case
        when fee_tier_key = 0 then 'default'
        when fee_tier_key = 1 then 'free_trial'
        when fee_tier_key = 2 then 'custom_fee'
        else 'unknown'
    end as fee_tier

from "dev"."staging"."stg_heropay_db__heropay_transactions"