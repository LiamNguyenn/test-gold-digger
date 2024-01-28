with staging as (
    select *

    from {{ ref("stg_ebenefits__bill_paid") }}
)

select
    bill_id                                                                                  as dim_bill_id,
    subscription_id                                                                          as dim_bill_management_subscription_id,
    {{ dbt_utils.generate_surrogate_key(["provider_id"]) }} as dim_bill_management_provider_sk,
    {{ get_date_id("transaction_date") }} as dim_date_sk,
    transaction_date,
    currency,
    bill_amount,
    paid_amount,
    total_saved,
    paid_amount * 0.01                                                                       as revenue_amount

from staging
