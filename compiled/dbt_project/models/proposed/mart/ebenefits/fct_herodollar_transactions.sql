select
    id, -- PK

    -- Foreign Keys
    

  to_number(to_char(transaction_date::DATE,'YYYYMMDD'),'99999999')

  as dim_date_sk,

    case when lower(transaction_initiator_type) = 'user' then transaction_initiator_id end         as dim_user_eh_user_id,
    case when lower(transaction_initiator_type) = 'organisation' then transaction_initiator_id end as dim_organisation_id,
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
    unified_transaction_amount,
    transaction_date,
    transaction_revenue_amount                                                                     as revenue_amount

from "dev"."intermediate"."int_calc_herodollar_revenue_margin"