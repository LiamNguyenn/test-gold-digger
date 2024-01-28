with source as (
    select *

    from {{ source("ebenefits", "hd_redeem_success_transactions") }}

    where
        source = 'HdRedeem'
        and _file like '%type=Redeem.Success%'
),

transformed as (
    select
        (
            case
                when json_extract_path_text(detail, 'id') = '' then NULL
                else json_extract_path_text(detail, 'id')
            end
        )::varchar      as id, -- noqa: RF04
        (
            case
                when json_extract_path_text(detail, 'transactionHayId') = '' then NULL
                else json_extract_path_text(detail, 'transactionHayId')
            end
        )::varchar      as hay_transaction_id,
        (
            case
                when json_extract_path_text(detail, 'redeemedAmount', 'amount') = ''
                    then
                        case
                            when json_extract_path_text(detail, 'currencyAmount', 'amount') = '' then NULL
                            else json_extract_path_text(detail, 'currencyAmount', 'amount')
                        end
                else json_extract_path_text(detail, 'redeemedAmount', 'amount')
            end
        )::float        as redeemed_amount,
        (
            case
                when json_extract_path_text(detail, 'feePercentage') = '' then NULL
                else json_extract_path_text(detail, 'feePercentage')
            end
        )::float        as fee_rate,
        (
            case
                when json_extract_path_text(detail, 'creditSpendAccountId') = '' then NULL
                else json_extract_path_text(detail, 'creditSpendAccountId')
            end
        )::varchar      as credit_spend_account_id,
        time::timestamp as transaction_time

    from source
)

select * from transformed
