{{ config(alias='paysplit_members') }}

with
    eh_wallet_users as (
        select 
            distinct eh_user_uuid
        from
            {{ ref('ebenefits_v_user_mapping') }} as m
            join {{ ref('ebenefits_wallet_customers') }} as w on
                m.eben_uuid = w.eben_uuid
        where m.eh_user_uuid is not null
    )
    -- eben paysplit log (doesnt capture full cohort), this is only used to capture the first date they became a paysplit user if entry exist
    , paysplit_eben_users as (
        select 
            m.eh_user_uuid, m.email, e.*
        from
            {{ ref('ebenefits_v_user_mapping') }} as m
            join {{first_row('dynamodb_eventlog', 'event_log', 'user_id', 'created_date')}} as e on
                m.eben_uuid = e.user_id
        where m.eh_user_uuid is not null
    )
    , paysplit_members as (
        select
            b.member_id
            , m.user_id
            , m.user_uuid
            , m.organisation_id
            , b.amount
            , b.account_name
            , case 
                when bank_split_type = 0 then 'percentage' 
                else 'fixed_amount' end as amount_type 
            , b.created_at as bank_account_created_at
            , b.updated_at as bank_account_updated_at
        from
            {{ source('postgres_public', 'bank_accounts') }} as b
            join {{ ref('employment_hero_employees') }} as m on 
                b.member_id = m.id
        where 
            b.bank_type = 'ewallet'
            and not b.disabled
            and not b._fivetran_deleted
    )

select 
    m.member_id
    , m.user_id
    , m.user_uuid
    , m.organisation_id
    , m.amount
    , m.account_name
    , m.amount_type 
    -- paysplit came out in october2022 but more than likely any early bank account created at may not be the paysplit but an user entering an different account and changing to paysplit later
    , case
        when u.created_at is not null then u.created_at
        when u.created_at is null and bank_account_created_at<'2023-01-01' then bank_account_updated_at
        else bank_account_created_at
    end as paysplit_created_at
    , case when wu.eh_user_uuid is not null then True else False end as has_wallet_account
from 
    paysplit_members m
    left join paysplit_eben_users u on u.eh_user_uuid = m.user_uuid
    left join eh_wallet_users wu on wu.eh_user_uuid = m.user_uuid