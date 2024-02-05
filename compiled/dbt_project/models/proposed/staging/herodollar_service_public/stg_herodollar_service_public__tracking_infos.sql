with source as (
    select *

    from "dev"."herodollar_service_public"."tracking_infos"
),

transformed as (
    select
        id::varchar                         as id, -- noqa: RF04
        hero_dollar_transaction_id::varchar as hero_dollar_transaction_id,
        ip_addresses::varchar               as ip_addressess,
        author_id::varchar                  as author_id,
        author_email::varchar               as author_email,
        reason_type::int                    as reason_type_key,
        reason_detail::varchar              as reason_detail,
        _fivetran_deleted::boolean          as fivetran_deleted,
        created_at::timestamp               as created_at,
        updated_at::timestamp               as updated_at

    from source
)

select * from transformed