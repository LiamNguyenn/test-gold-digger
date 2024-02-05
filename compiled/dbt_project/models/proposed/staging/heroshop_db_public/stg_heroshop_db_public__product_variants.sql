with source as (
    select *

    from "dev"."heroshop_db_public"."product_variants"
),

transformed as (
    select
        id::varchar                       as id, -- noqa: RF04
        product_id::varchar               as product_id,
        variant_code::varchar             as variant_code,
        price::float                      as price,
        created_at::timestamp             as created_at,
        updated_at::timestamp             as updated_at,
        name::varchar                     as name, -- noqa: RF04
        image_url::varchar                as image_url,
        disabled::boolean                 as disabled,
        discounted_price::float           as discounted_price,
        _fivetran_synced::timestamp       as fivetran_synced,
        _fivetran_deleted::boolean        as fivetran_deleted,
        rrp::float                        as rrp,
        supplier_price::float             as supplier_price,
        freight_price::float              as freight_price,
        status::int                       as status,
        label::varchar                    as label, -- noqa: RF04
        supplier_product_id::varchar      as supplier_product_id,
        local_id::int                     as local_id,
        card_id::varchar                  as card_id,
        image::varchar                    as image,
        position::int                     as position, -- noqa: RF04
        stock_status::int                 as stock_status,
        giftaway_denomination_id::varchar as giftaway_denomination_id,
        uber_sku::varchar                 as uber_sku

    from source
)

select * from transformed
where not fivetran_deleted