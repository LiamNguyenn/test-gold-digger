with source as (
    select *

    from "dev"."heroshop_db_public"."transactions"
),

transformed as (
    select
        id::varchar                  as id, -- noqa: RF04
        order_id::varchar            as order_id,
        customer_id::varchar         as customer_id,
        transaction_id::varchar      as transaction_id,
        status::varchar              as status,
        currency::varchar            as currency_code,
        amount::float                as transaction_amount,
        points::float                as hero_points_amount,
        transaction_fee::float       as transaction_fee,
        fee_percent::float           as fee_rate,
        payment_method::int          as payment_method_key,
        error_code::varchar          as error_code,
        error_message::varchar       as error_message,
        three_d_secure_info::varchar as three_d_secure_info,
        _fivetran_deleted::boolean   as fivetran_deleted,
        created_at::timestamp        as created_at,
        updated_at::timestamp        as updated_at

    from source
)

select * from transformed
where not fivetran_deleted