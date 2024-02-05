with enriched_heroshop_orders as (
    select
        orders.id,
        orders.created_at::date                                     as order_date,
        

  to_number(to_char(orders.created_at::DATE,'YYYYMMDD'),'99999999')

                                                                                                                          as dim_date_sk,
        orders.member_id                                            as dim_employee_eh_employee_id,
        orders.service_fee,
        orders.billable_amount,
        orders.transaction_fee,
        orders.freight_cost,
        orders.promo_total
    from "dev"."staging"."stg_heroshop_db_public__orders" as orders
)

select
    orders.id,
    orders.order_date,
    orders.dim_date_sk,
    orders.dim_employee_eh_employee_id,
    orders.service_fee,
    orders.billable_amount,
    orders.transaction_fee,
    orders.freight_cost,
    orders.promo_total
from enriched_heroshop_orders as orders