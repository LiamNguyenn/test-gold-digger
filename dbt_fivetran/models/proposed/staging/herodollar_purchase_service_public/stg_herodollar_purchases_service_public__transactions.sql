with source as (
    select *

    from {{ source("herodollar_purchase_service_public", "transactions") }}
),

transformed as (
    select
        id::varchar                               as id, -- noqa: RF04
        transaction_id::int                       as transaction_id,
        payee_id::varchar                         as payee_id,
        payer_id::varchar                         as payer_id,
        payment_processor_transaction_id::varchar as payment_processor_transaction_id,
        payment_processor::int                    as payment_processor_key,
        payee_type::varchar                       as payee_type,
        ip_addresses::varchar                     as ip_addressess,
        status::varchar                           as status,
        purchase_method::varchar                  as purchase_method,
        amount::float                             as transaction_amount,
        points::float                             as heropoints_amount,
        rate::float                               as rate,
        net_amount::float                         as net_transaction_amount,
        _fivetran_deleted::boolean                as fivetran_deleted,
        created_at::timestamp                     as created_at,
        updated_at::timestamp                     as updated_at

    from source
)

select * from transformed
