{% set columns_list = [
    {"id": "bill_id"},
    {"subscriptionId": "subscription_id"},
    {"providerId": "provider_id"},
    {"type": "type"},
    {"providerTransactionId": "provider_transaction_id"},
    {"transactionDate": "transaction_date"},
    {"currency": "currency"},
    {"amount": "paid_amount"},
    {"totalSaved": "total_saved"}
] %}

with source as (
    select *

    from {{ source("ebenefits", "payment_created") }}

),

transformed as (
    select
        id::varchar                                                                                                                                  as id, --noqa: RF04
        detail_type::varchar                                                                                                                         as detail_type,
        source::varchar                                                                                                                              as source, --noqa: RF04
        time::timestamp                                                                                                                              as created_at,
        {{ extract_all_json_elements(columns_list, "detail") }}

    from source
)

select * from transformed
