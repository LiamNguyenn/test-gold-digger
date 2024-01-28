with mapped_herodollar_transactions as (
    select
        transactions.id,
        transactions.transaction_initiator_id,
        transactions.ref_id,
        transactions.parent_id,
        transactions.transaction_source,
        transactions.transaction_initiator_type,
        transaction_type.transaction_type,
        reason_type.reason_type,
        transactions.currency_code,
        transactions.is_hero_points_transaction,
        transactions.hero_dollar_amount,
        transactions.hero_points_amount,
        transactions.hero_points_conversion_rate,
        transactions.created_at::date as transaction_date

    from {{ ref("stg_herodollar_service_public__herodollar_transactions") }} as transactions

    inner join {{ ref("int_map_herodollar_transaction_type") }} as transaction_type
        on transactions.transaction_type_key = transaction_type.transaction_type_key

    left join {{ ref("stg_herodollar_service_public__tracking_infos") }} as tracking
        on transactions.id = tracking.hero_dollar_transaction_id

    left join {{ ref("int_map_herodollar_reason_type_key") }} as reason_type
        on tracking.reason_type_key = reason_type.reason_type_key

    where not transactions.fivetran_deleted
),

unified_transaction_amount as (
    select
        id,
        transaction_initiator_id,
        ref_id,
        parent_id,
        transaction_source,
        transaction_initiator_type,
        transaction_type,
        reason_type,
        currency_code,
        is_hero_points_transaction,
        hero_dollar_amount,
        hero_points_amount,
        hero_points_conversion_rate,
        transaction_date,
        case
            when is_hero_points_transaction then hero_points_amount * hero_points_conversion_rate
            else hero_dollar_amount
        end as unified_transaction_amount

    from mapped_herodollar_transactions
),

transactions_with_margin_and_revenue as (
    select
        herodollar_transactions.*,
        case
            when herodollar_transactions.is_hero_points_transaction
                then
                    case
                        when herodollar_transactions.transaction_source = 'ebf_shaype' then -abs(redeem_success_transactions.redeemed_amount)
                        when herodollar_transactions.transaction_source = 'marketplace' then -abs(heroshop_transactions.transaction_amount)
                        when herodollar_transactions.transaction_source = 'hero_dollar_purchase' then herodollar_purchase.net_transaction_amount
                        else herodollar_transactions.unified_transaction_amount
                    end
            else herodollar_transactions.unified_transaction_amount
        end as transaction_amount_margin_rate,
        case
            when herodollar_transactions.is_hero_points_transaction
                then
                    case
                        when herodollar_transactions.transaction_source in ('ebf_shaype', 'marketplace', 'hero_dollar_purchase') then abs(herodollar_transactions.unified_transaction_amount - transaction_amount_margin_rate)
                        else 0
                    end
            when herodollar_transactions.transaction_source = 'ebf_shaype' and herodollar_transactions.reason_type = 'transaction_fee' then abs(herodollar_transactions.unified_transaction_amount)
            else 0
        end as transaction_revenue_amount

    from unified_transaction_amount as herodollar_transactions

    left join {{ ref("stg_ebenefits__hd_redeem_success_transactions") }} as redeem_success_transactions
        on
            herodollar_transactions.ref_id = redeem_success_transactions.id
            and herodollar_transactions.transaction_source = 'ebf_shaype'
            and herodollar_transactions.reason_type = 'default'

    left join {{ ref("stg_heroshop_db_public__transactions") }} as heroshop_transactions
        on
            herodollar_transactions.ref_id = heroshop_transactions.order_id
            and herodollar_transactions.transaction_source = 'marketplace'
            and heroshop_transactions.status = 'success'

    left join {{ ref("int_map_heroshop_payment_method") }} as heroshop_payment_method
        on
            heroshop_transactions.payment_method_key = heroshop_payment_method.payment_method_key
            and heroshop_payment_method.payment_method in ('hero_dollars', 'hero_points')

    left join {{ ref("stg_herodollar_purchases_service_public__transactions") }} as herodollar_purchase
        on
            herodollar_transactions.ref_id = herodollar_purchase.id
            and herodollar_transactions.transaction_source = 'hero_dollar_purchase'
)

select * from transactions_with_margin_and_revenue
