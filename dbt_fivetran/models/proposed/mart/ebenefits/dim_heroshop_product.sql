select
    id           as dim_heroshop_product_id,
    name         as product_name,
    product_code,
    country      as product_country,
    product_type as product_type_enum,
    case
        when product_type_enum = 0 then 'grocery'
        when product_type_enum = 1 then 'ticket'
        when product_type_enum = 2 then 'giftcard'
        when product_type_enum = 3 then 'dropship'
        else 'Unknown'
    end          as product_type

from {{ ref("stg_heroshop_db_public__products") }}
