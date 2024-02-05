select distinct
    payment_processor_key,
    case
        when payment_processor_key = 0 then 'braintree'
        when payment_processor_key = 1 then 'stripe'
        else 'unknown'
    end as payment_processor

from "dev"."staging"."stg_herodollar_purchases_service_public__transactions"