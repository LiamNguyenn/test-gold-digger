with enriched_instapay_transactions as (
    select
        heropay.id,
        heropay.employee_uuid,
        transaction_status.transaction_status,
        fee_tier.fee_tier,
        heropay.transaction_amount,
        heropay.fee_charged,
        heropay.created_at::date as transaction_date

    from "dev"."staging"."stg_heropay_db__heropay_transactions" as heropay

    left join "dev"."intermediate"."int_map_instapay_fee_tier" as fee_tier
        on heropay.fee_tier_key = fee_tier.fee_tier_key

    left join "dev"."intermediate"."int_map_instapay_transaction_status" as transaction_status
        on heropay.transaction_status_key = transaction_status.transaction_status_key

    where not heropay.fivetran_deleted
)

select
    instapay.id, -- PK
    instapay.employee_uuid                                         as dim_employee_uuid,
    instapay.transaction_date,
    instapay.transaction_status,
    instapay.fee_tier,
    instapay.transaction_amount,
    instapay.fee_charged                                           as revenue_amount,
    

  to_number(to_char(transaction_date::DATE,'YYYYMMDD'),'99999999')

 as dim_date_sk

from enriched_instapay_transactions as instapay