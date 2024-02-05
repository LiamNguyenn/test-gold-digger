

with source as (
    select *

    from "dev"."ebenefits"."payment_created"

),

transformed as (
    select
        id::varchar                                                                                                                                  as id, --noqa: RF04
        detail_type::varchar                                                                                                                         as detail_type,
        source::varchar                                                                                                                              as source, --noqa: RF04
        time::timestamp                                                                                                                              as created_at,
        



    case when json_extract_path_text(detail, 'id')= '' then null else json_extract_path_text(detail, 'id') end as bill_id
    
    ,

    case when json_extract_path_text(detail, 'subscriptionId')= '' then null else json_extract_path_text(detail, 'subscriptionId') end as subscription_id
    
    ,

    case when json_extract_path_text(detail, 'providerId')= '' then null else json_extract_path_text(detail, 'providerId') end as provider_id
    
    ,

    case when json_extract_path_text(detail, 'type')= '' then null else json_extract_path_text(detail, 'type') end as type
    
    ,

    case when json_extract_path_text(detail, 'providerTransactionId')= '' then null else json_extract_path_text(detail, 'providerTransactionId') end as provider_transaction_id
    
    ,

    case when json_extract_path_text(detail, 'transactionDate')= '' then null else json_extract_path_text(detail, 'transactionDate') end as transaction_date
    
    ,

    case when json_extract_path_text(detail, 'currency')= '' then null else json_extract_path_text(detail, 'currency') end as currency
    
    ,

    case when json_extract_path_text(detail, 'amount')= '' then null else json_extract_path_text(detail, 'amount') end as paid_amount
    
    ,

    case when json_extract_path_text(detail, 'totalSaved')= '' then null else json_extract_path_text(detail, 'totalSaved') end as total_saved
    
    



    from source
)

select * from transformed