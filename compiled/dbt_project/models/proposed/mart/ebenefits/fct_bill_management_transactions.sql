with staging as (
    select *

    from "dev"."staging"."stg_ebenefits__bill_paid"
)

select
    bill_id                                                                                  as dim_bill_id,
    subscription_id                                                                          as dim_bill_management_subscription_id,
    md5(cast(coalesce(cast(provider_id as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as dim_bill_management_provider_sk,
    

  to_number(to_char(transaction_date::DATE,'YYYYMMDD'),'99999999')

 as dim_date_sk,
    transaction_date,
    currency,
    bill_amount,
    paid_amount,
    total_saved,
    paid_amount * 0.01                                                                       as revenue_amount

from staging