with source as (
    select *

    from "dev"."herodollar_service_public"."hero_dollar_transactions"
),

transformed as (
    select
        id::varchar                 as id, -- noqa: RF04
        transactable_id::varchar    as transaction_initiator_id,
        ref_id::varchar             as ref_id,
        parent_id::varchar          as parent_id,
        transactable_type::varchar  as transaction_initiator_type,
        transaction_type::int       as transaction_type_key,
        client_type::varchar        as transaction_source,
        "identity"::varchar         as transaction_initiator_identity,
        currency_code::varchar      as currency_code,
        hero_points::boolean        as is_hero_points_transaction,
        amount::float               as hero_dollar_amount,
        points::float               as hero_points_amount,
        base_conversion_rate::float as hero_points_conversion_rate,
        margin_percentage::float    as transaction_margin_rate,
        _fivetran_deleted::boolean  as fivetran_deleted,
        created_at::timestamp       as created_at,
        updated_at::timestamp       as updated_at

    from source
)

select * from transformed