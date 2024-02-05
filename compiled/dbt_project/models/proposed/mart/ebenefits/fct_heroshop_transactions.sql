with enriched_heroshop_transactions as (
    select
        transactions.id,
        transactions.order_id,
        transactions.status,
        transactions.created_at::date                                     as transaction_date,
        

  to_number(to_char(transactions.created_at::DATE,'YYYYMMDD'),'99999999')

 as dim_date_sk,
        payment_method.payment_method,
        transactions.transaction_amount,
        transactions.hero_points_amount,
        transactions.transaction_fee,
        transactions.fee_rate,
        transactions.currency_code



    from "dev"."staging"."stg_heroshop_db_public__transactions" as transactions
    left join "dev"."intermediate"."int_map_heroshop_payment_method" as payment_method on transactions.payment_method_key = payment_method.payment_method_key

)

select
    transactions.id,
    transactions.order_id,
    transactions.status,
    transactions.transaction_date,
    transactions.dim_date_sk,
    transactions.payment_method,
    transactions.transaction_amount,
    transactions.hero_points_amount,
    transactions.transaction_fee,
    transactions.fee_rate,
    transactions.currency_code

from enriched_heroshop_transactions as transactions