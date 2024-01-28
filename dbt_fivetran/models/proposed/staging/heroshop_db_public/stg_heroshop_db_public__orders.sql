with source as (
    select *

    from {{ source("heroshop_db_public", "orders") }}
),

transformed as (
    select
        id::varchar                 as id, -- noqa: RF04
        member_id::varchar          as member_id,
        billable_amount::float      as billable_amount,
        status::varchar             as status,
        created_at::timestamp       as created_at,
        updated_at::timestamp       as updated_at,
        ip_address::varchar         as ip_address,
        transaction_fee::float      as transaction_fee,
        _fivetran_synced::timestamp as fivetran_synced,
        _fivetran_deleted::boolean  as fivetran_deleted,
        local_id::int               as local_id,
        freight_cost::float         as freight_cost,
        ip_addresses::varchar       as ip_addresses,
        promo_total::float          as promo_total,
        payment_params::varchar     as payment_params,
        service_fee::float          as service_fee,
        platform::varchar           as platform

    from source
)

select * from transformed
where not fivetran_deleted
