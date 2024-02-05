select distinct
    referred_from_key,
    case
        when referred_from_key = 0 then 'client'
        when referred_from_key = 1 then 'marketplace'
        when referred_from_key = 2 then 'swag'
        else 'unknown'
    end as referrer_source

from "dev"."staging"."stg_heropay_db__heropay_transactions"