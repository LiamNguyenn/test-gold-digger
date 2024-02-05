with source as (
    select *

    from "dev"."heroshop_db_public"."order_details"
),

transformed as (
    select
        id::varchar                 as id, -- noqa: RF04
        order_id::varchar           as order_id,
        discount::float             as discount,
        quantity::int               as quantity,
        subtotal::float             as subtotal,
        billable_amount::float      as billable_amount,
        price::float                as price,
        created_at::timestamp       as created_at,
        updated_at::timestamp       as updated_at,
        product_variant_id::varchar as product_variant_id,
        _fivetran_synced::timestamp as fivetran_synced,
        _fivetran_deleted::boolean  as fivetran_deleted,
        transaction_fee::float      as transaction_fee,
        tracking_url::varchar       as tracking_url,
        serial_number::varchar      as serial_number,
        supplier_price::float       as supplier_price,
        freight_cost::float         as freight_cost,
        status::varchar             as status,
        local_id::int               as local_id

    from source

)

select * from transformed
where not fivetran_deleted