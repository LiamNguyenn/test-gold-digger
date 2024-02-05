with eh_cashback_network_txs as (
    select
        md5(cast(coalesce(cast(transaction_date as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(user_email as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as transaction_id,
        transaction_date,
        campaign_name,
        user_email,
        original_amount,
        fee,
        cashback,
        'eh'                                                                                                                                                                         as source,
        split_part(user_email, '_', 1)                                                                                                                                               as eben_uuid
    from "dev"."ebenefits"."eh_cashback_network_txs" as t1
    where t1._modified = (
        select max(_modified)
        from "dev"."ebenefits"."eh_cashback_network_txs"
        where _file = t1._file
    )
),

cashback_transactions as (
    select
        md5(cast(coalesce(cast(transaction_date as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(user_email as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as transaction_id,
        transaction_date,
        campaign_name,
        user_email,
        original_amount,
        fee,
        cashback,
        'pokitpal'                                                                                                                                                                   as source,
        split_part(user_email, '_', 1)                                                                                                                                               as eben_uuid
    from "dev"."ebenefits"."pokitpal_cashback_network_txs" as t1
    where t1._modified = (
        select max(_modified)
        from "dev"."ebenefits"."pokitpal_cashback_network_txs"
        where _file = t1._file
    )
),

enrich_uuid as (
    select
        tx.*,
        eben_user.eh_user_uuid as user_uuid
    from (
        select * from eh_cashback_network_txs
        union all
        select * from cashback_transactions
    ) as tx
    left join "dev"."ebenefits"."_v_user_mapping" as eben_user
        on tx.eben_uuid = eben_user.eben_uuid
    where eben_user.eh_user_uuid is not null
),

final as (
    select
        transaction_id                  as event_id,
        user_uuid,
        cast('user_transacted' as text) as event_name,
        transaction_date                as event_time,
        campaign_name                   as retailer,
        'Cashback'                      as type_of_offer,
        cashback                        as cashback_received
    from enrich_uuid
    where 1 = 1
    qualify row_number() over (partition by transaction_id order by event_time desc) = 1
)

select *
from final