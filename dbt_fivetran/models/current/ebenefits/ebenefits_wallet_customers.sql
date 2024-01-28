{{ config(alias='wallet_customers') }}

select
    id
    , cc.time::timestamp as created_at
    , (case when json_extract_path_text(detail, 'accountHayId')= '' then null else json_extract_path_text(detail, 'accountHayId') end) as account_hay_id
    , (case when json_extract_path_text(detail, 'hayCustomerId')= '' then null else json_extract_path_text(detail, 'hayCustomerId') end) as hay_customer_id
    , (case when json_extract_path_text(detail, 'userId')= '' then null else json_extract_path_text(detail, 'userId') end) as eben_uuid
from
    {{ source('ebenefits', 'customer_created') }} as cc