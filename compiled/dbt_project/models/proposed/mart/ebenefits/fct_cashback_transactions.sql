select
    transaction_timestamp,
    processed_timestamp,
    

  to_number(to_char(transaction_timestamp::DATE,'YYYYMMDD'),'99999999')

 as dim_date_sk,
    'employment_hero'                                                               as cashback_network,
    merchant_name,
    regexp_substr(user_email, '[0-9A-fa-f]{8}(-[0-9A-fa-f]{4}){3}-[0-9A-fa-f]{12}') as dim_user_ebenefits_user_uuid,
    transaction_amount,
    revenue_amount,
    cashback_amount

from "dev"."staging"."stg_ebenefits__cashback_transactions_eh"

union distinct

select
    transaction_timestamp,
    processed_timestamp,
    

  to_number(to_char(transaction_timestamp::DATE,'YYYYMMDD'),'99999999')

 as dim_date_sk,
    'pokitpal'                                                                      as cashback_network,
    merchant_name,
    regexp_substr(user_email, '[0-9A-fa-f]{8}(-[0-9A-fa-f]{4}){3}-[0-9A-fa-f]{12}') as dim_user_ebenefits_user_uuid,
    transaction_amount,
    revenue_amount,
    cashback_amount

from "dev"."staging"."stg_ebenefits__cashback_transactions_pokitpal"