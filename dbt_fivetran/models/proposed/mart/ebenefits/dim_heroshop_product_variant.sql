select
    id as dim_heroshop_product_variant_id,
    variant_code,
    price,
    discounted_price,
    rrp,
    supplier_price,
    freight_price
from {{ ref("stg_heroshop_db_public__product_variants") }}
