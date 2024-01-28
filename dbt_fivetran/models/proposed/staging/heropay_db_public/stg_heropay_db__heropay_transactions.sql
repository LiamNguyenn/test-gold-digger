with source as (
    select *

    from {{ source("heropay_db_public", "heropay_transactions") }}
),

transformed as (
    select
        id::varchar                      as id, -- noqa: RF04
        heropay_balance_id::varchar      as heropay_balance_id,
        member_id::varchar               as employee_uuid,
        historical::boolean              as historical,
        status::int                      as transaction_status_key,
        amount::float                    as transaction_amount,
        admin_fee::float                 as fee_charged,
        fee_type::int                    as fee_tier_key,
        aba_description::varchar         as aba_file_description,
        aba_lodgement_reference::varchar as aba_file_lodgement_reference,
        aba_sent::boolean                as aba_sent,
        aba_status::varchar              as aba_status,
        aba_name::varchar                as aba_file_name,
        aba_url::varchar                 as aba_file_url,
        ip_addresses::varchar            as ip_address,
        comment::varchar                 as comment, -- noqa: RF04
        ref_id::int                      as ref_id,
        referred_from::int               as referred_from_key,
        referred_id::varchar             as referred_id,
        created_at::timestamp            as created_at,
        updated_at::timestamp            as updated_at,
        deleted_at::timestamp            as deleted_at,
        aba_sent_at::timestamp           as aba_sent_at,
        _fivetran_deleted::boolean       as fivetran_deleted

    from source
)

select * from transformed
