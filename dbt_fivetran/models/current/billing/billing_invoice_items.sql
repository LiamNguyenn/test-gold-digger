{{ config(alias='invoice_items') }}

select
  f.id as organisation_id
  , invoice.account_id as zuora_account_id
  , a.account_number as zuora_account_num
  , a.name as zuora_account_name
  , invoice_item.subscription_id
  , invoice.id as invoice_id
  , invoice.invoice_date
  , invoice.status as invoice_status
  , invoice.posted_date as invoice_posted_date
  , invoice_item.charge_name
  , invoice_item.quantity
  , invoice_item.charge_amount
  , invoice_item.tax_amount
  , p.name as product_name
  , prp.name as product_rate_plan
  , dense_rank() over(partition by invoice.account_id order by invoice.posted_date asc) invoice_order
from
  {{ ref('employment_hero_organisations') }} as f
  join {{ source('zuora', 'account') }} a on f.zuora_account_id = a.id
  join {{ source('zuora', 'invoice') }} on a.id = invoice.account_id
  join {{ source('zuora', 'invoice_item') }} on invoice.id = invoice_item.invoice_id
  join {{ source('zuora', 'rate_plan_charge') }} rpc on rpc.id = invoice_item.rate_plan_charge_id
  join {{ source('zuora', 'product_rate_plan') }} prp on rpc.product_rate_plan_id = prp.id
  join {{ source('zuora', 'product') }} p on p.id = prp.product_id
where 
  not a._fivetran_deleted
  and not invoice._fivetran_deleted                       
  and not invoice_item._fivetran_deleted
  and not p._fivetran_deleted
  and not prp._fivetran_deleted
  and not rpc._fivetran_deleted