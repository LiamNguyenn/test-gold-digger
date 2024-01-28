{{ config(materialized='view', alias='_v_heroshop_transactions_order_details') }}

select
  t.id as transactions_id,
  o.id as order_id,
  od.id as order_details_id,
  od.created_at,
  m.id as member_id,
  m.user_id as user_id,
  m.organisation_id,
  p.name,
  p.country,
  pc.name as product_category,
  pv.variant_code,
  od.status as order_status,
  od.quantity,
  od.price,
  od.price*quantity as total_price,
  od.freight_cost,
  od.discount,
  o.service_fee,
  od.billable_amount,
  case 
    when t.payment_method = 1 then 'Instapay'
    when t.payment_method = 2 then 'HeroDollars'
    else 'Credit Card' end
  as payment_method,
  t.transaction_fee,
  t.amount,
  t.transaction_fee + t.amount as amount_paid,
  od.discount-od.transaction_fee as savings,
  t.status as transaction_status
from 
  {{ source('heroshop_db_public', 'order_details') }} od
  join {{ source('heroshop_db_public', 'product_variants') }} pv on pv.id = od.product_variant_id
  join {{ source('heroshop_db_public', 'orders') }} o on od.order_id = o.id
  join {{ source('heroshop_db_public', 'products') }} p on p.id = pv.product_id
  join {{ source('postgres_public', 'members') }} m on 
    m.uuid = o.member_id
    and not m.is_shadow_data 
    and not m._fivetran_deleted
  join {{ source('postgres_public', 'organisations') }} org on 
    m.organisation_id = org.id
    and not org._fivetran_deleted
  join {{ source('heroshop_db_public', 'transactions') }} t on o.id = t.order_id
  join {{ source('heroshop_db_public', 'product_categories') }} pc on p.product_category_id = pc.id