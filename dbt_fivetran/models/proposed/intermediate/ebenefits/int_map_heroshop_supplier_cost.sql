select
    id  as product_variant_id,
    case
        when supplier_price is not NULL then supplier_price
        when variant_code ~* '-50' then 47.37
        when variant_code !~* 'EMOVIE|ESAVER' then 94.74
        when variant_code = 'ESAVER_CHILD' then 10.5
        when variant_code = 'EMOVIE_ADULT' then 15.5
        when variant_code = 'ESAVER_ADULT' then 12.5
        when variant_code = 'EMOVIE_CHILD' then 12.5
        else 0
    end as supplier_cost
from {{ ref("stg_heroshop_db_public__product_variants") }}
