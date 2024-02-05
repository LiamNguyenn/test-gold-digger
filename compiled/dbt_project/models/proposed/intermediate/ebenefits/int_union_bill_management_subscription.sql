with submitted as (
    select
        submitted.*,
        'submitted' as subscription_status

    from "dev"."staging"."stg_ebenefits__subscription_submitted" as submitted

    qualify row_number() over (partition by subscription_id order by created_at) = 1 -- take the earliest to get "created_at" timestamp of the final dim table
),

active as (
    select
        active.*,
        'active' as subscription_status

    from "dev"."staging"."stg_ebenefits__subscription_active" as active

    qualify row_number() over (partition by subscription_id order by created_at) = 1 -- take the earliest to get "updated_at" timestamp of the final dim table; the event bucket seems to be duplicated for the same sub ID
),

cancelled as (
    select
        cancelled.*,
        'cancelled' as subscription_status

    from "dev"."staging"."stg_ebenefits__subscription_cancelled" as cancelled

    qualify row_number() over (partition by subscription_id order by created_at) = 1 -- take the earliest to get "updated_at" timestamp of the final dim table; the event bucket seems to be duplicated for the same sub ID
),

unioned as (
    select
        subscription_id,
        ebenefits_user_uuid,
        provider_id,
        NULL       as external_id,
        NULL       as external_user_id,
        subscription_type,
        subscription_status,
        created_at as submitted_at,
        NULL       as activated_at,
        NULL       as cancelled_at,
        created_at,
        created_at as updated_at

    from submitted

    union distinct

    select
        subscription_id,
        ebenefits_user_uuid,
        provider_id,
        external_id,
        external_user_id,
        subscription_type,
        subscription_status,
        NULL       as submitted_at,
        created_at as activated_at,
        NULL       as cancelled_at,
        NULL       as created_at,
        created_at as updated_at

    from active

    union distinct

    select
        subscription_id,
        ebenefits_user_uuid,
        provider_id,
        external_id,
        external_user_id,
        subscription_type,
        subscription_status,
        NULL       as submitted_at,
        NULL       as activated_at,
        created_at as cancelled_at,
        NULL       as created_at,
        created_at as updated_at

    from cancelled
),

deduped as (
    select
        last_value(subscription_id ignore nulls) over (partition by subscription_id order by created_at rows between unbounded preceding and unbounded following)     as subscription_id,
        last_value(ebenefits_user_uuid ignore nulls) over (partition by subscription_id order by created_at rows between unbounded preceding and unbounded following) as ebenefits_user_uuid,
        last_value(provider_id ignore nulls) over (partition by subscription_id order by created_at rows between unbounded preceding and unbounded following)         as provider_id,
        last_value(external_id ignore nulls) over (partition by subscription_id order by created_at rows between unbounded preceding and unbounded following)         as external_id,
        last_value(external_user_id ignore nulls) over (partition by subscription_id order by created_at rows between unbounded preceding and unbounded following)    as external_user_id,
        last_value(subscription_type ignore nulls) over (partition by subscription_id order by created_at rows between unbounded preceding and unbounded following)   as subscription_type,
        last_value(subscription_status ignore nulls) over (partition by subscription_id order by created_at rows between unbounded preceding and unbounded following) as subscription_status,
        last_value(submitted_at ignore nulls) over (partition by subscription_id order by created_at rows between unbounded preceding and unbounded following)        as submitted_at,
        last_value(activated_at ignore nulls) over (partition by subscription_id order by created_at rows between unbounded preceding and unbounded following)        as activated_at,
        last_value(cancelled_at ignore nulls) over (partition by subscription_id order by created_at rows between unbounded preceding and unbounded following)        as cancelled_at,
        last_value(created_at ignore nulls) over (partition by subscription_id order by created_at rows between unbounded preceding and unbounded following)          as created_at,
        last_value(updated_at ignore nulls) over (partition by subscription_id order by created_at rows between unbounded preceding and unbounded following)          as updated_at

    from unioned
)

select *

from deduped

group by 1,2,3,4,5,6,7,8,9,10,11,12