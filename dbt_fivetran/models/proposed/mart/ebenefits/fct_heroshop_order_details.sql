with enriched_heroshop_order_details as (
    select
        order_details.id,
        order_details.created_at::date                                                                                                                      as order_date,
        {{ get_date_id("order_details.created_at") }}                                                                                                                          as dim_date_sk,
        order_details.order_id,
        order_details.product_variant_id                                                                                                                    as dim_heroshop_product_variant_id,
        orders.member_id                                                                                                                                    as dim_employee_eh_employee_id,
        product_variants.product_id                                                                                                                         as dim_heroshop_product_id,
        products.product_category_id                                                                                                                        as dim_product_category_product_category_id,
        suppliers.id                                                                                                                                        as dim_supplier_id,
        order_details.local_id,
        order_details.discount,
        order_details.quantity,
        order_details.subtotal,
        order_details.billable_amount,
        order_details.price,
        order_details.transaction_fee,
        order_details.supplier_price,
        order_details.freight_cost,
        order_details.price * order_details.quantity                                                                                                        as total_price,
        supplier_cost.supplier_cost,
        order_details.discount - order_details.transaction_fee                                                                                              as savings,
        -- (order_details.billable_amount) - (order_details.transaction_fee + order_details.freight_cost) - (supplier_cost.supplier_cost * order_details.quantity) as revenue,
        order_details.billable_amount - (order_details.quantity * supplier_cost.supplier_cost) - order_details.freight_cost - order_details.transaction_fee as margin,
        order_details.status


    from {{ ref("stg_heroshop_db_public__order_details") }} as order_details
    left join {{ ref("int_map_heroshop_supplier_cost") }} as supplier_cost on order_details.product_variant_id = supplier_cost.product_variant_id
    left join {{ ref("stg_heroshop_db_public__orders") }} as orders on order_details.order_id = orders.id
    left join {{ ref("stg_heroshop_db_public__product_variants") }} as product_variants on order_details.product_variant_id = product_variants.id
    left join {{ ref("stg_heroshop_db_public__products") }} as products on product_variants.product_id = products.id
    left join {{ ref("stg_heroshop_db_public__product_categories") }} as product_categories on products.product_category_id = product_categories.id
    left join {{ ref("stg_heroshop_db_public__suppliers") }} as suppliers on products.supplier_id = suppliers.id
    --add member details and org details when tables are ready

)

select

    order_details.id,
    order_details.order_date,
    order_details.dim_date_sk,
    order_details.order_id,
    order_details.dim_heroshop_product_variant_id,
    order_details.dim_employee_eh_employee_id,
    order_details.dim_heroshop_product_id,
    order_details.dim_product_category_product_category_id,
    order_details.dim_supplier_id,
    order_details.local_id,
    order_details.discount,
    order_details.quantity,
    order_details.subtotal,
    order_details.billable_amount,
    order_details.price,
    order_details.transaction_fee,
    order_details.supplier_price,
    order_details.freight_cost,
    order_details.total_price,
    order_details.supplier_cost,
    order_details.savings,
    -- order_details.revenue,
    order_details.margin,
    order_details.status

from enriched_heroshop_order_details as order_details
