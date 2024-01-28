{{ config(alias='users') }}

with
  eh_hr_accounts as (
    -- this view gets all accounts that were or are being billed for EH HR Software
    select 
      distinct(sa.id) as external_id
      , sa.name
    from
      {{ source('zuora', 'account') }} za
      inner join {{ source('salesforce', 'account') }} sa on
        za.crm_id = sa.id
      inner join {{ source('zuora', 'subscription') }} zs on
        zs.account_id = za.id
      left join {{ source('zuora', 'rate_plan_charge') }} zrpc on
        zs.id = zrpc.subscription_id
      left join {{ source('zuora', 'product_rate_plan') }} zprp on
        zrpc.product_rate_plan_id = zprp.id
      left join {{ source('zuora', 'product') }} zp on
        zprp.product_id = zp.id
    where
      -- test accounts must be hard coded out of this view 
      -- hardcode is the best way to do this so accounts with 'test' within the name is not accidentally removed, ie. contest
      za.batch != 'Batch50'
      and not za._fivetran_deleted
      and not sa.is_deleted
      and not zs._fivetran_deleted
      and not zp._fivetran_deleted
      and not zprp._fivetran_deleted
      and not zrpc._fivetran_deleted      
      -- limit to test accounts
      --and sa.id in (select external_id from customers.accounts)
  )
, user_details as(
    select distinct
      eha.external_id as account_list
      , eha.name as account_name
      , u.uuid as user_uuid
      , u.email as account_email
      , m.company_email
      , nvl(case when m.company_email is null or m.company_email = '' then u.email else lower(m.company_email) end, u.email) as email
      , u.created_at
      , m.id as member_id
      , m.created_at member_created_at
      , initcap(m.first_name || ' ' || m.last_name) as member_name
      , m.organisation_id
      , role
      , active
      , accepted
      , his.title job_title
      , m.independent_contractor
      , m.termination_date
      , u._fivetran_deleted as user_fivetran_deleted
      , m._fivetran_deleted as member_fivetran_deleted
      , listagg(distinct trim(epa.type, 'Auth'), ', ') payroll_type
    from
      eh_hr_accounts eha
      inner join {{ source('zuora', 'account') }} za on
        za.crm_id = eha.external_id
      join {{ source('postgres_public', 'organisations') }} o on
        za.id = o.zuora_account_id
      join {{ source('postgres_public', 'members') }} m on
        o.id = m.organisation_id
      join {{ source('postgres_public', 'users') }} u on
        m.user_id = u.id
      left join (
        select member_id, title
          from {{ source('postgres_public', 'employment_histories') }}
          where id in (
            select
              FIRST_VALUE(id) over (
                partition by member_id order by created_at desc 
                  rows between unbounded preceding and current row
               )
            from
              {{ source('postgres_public', 'employment_histories') }}
            where not _fivetran_deleted
          )
      ) as his on
        m.id = his.member_id
      left join {{ref('employment_hero_v_connected_payrolls')}} epa on
        m.organisation_id = epa.organisation_id
    where
      not o._fivetran_deleted
      and ( not u._fivetran_deleted
           or (u._fivetran_deleted
               and u._fivetran_synced > 2022) )
 -- deleted users before 2022 are not imported to Vitally, no need to update status                
      and not za._fivetran_deleted
      -- and not m._fivetran_deleted
      and email !~* '.*(employmenthero|employmentinnovations|thinkei|p2m|power2motivate|test|demo|exacc|sandbox|\\+).*'
      and not m.system_manager
      and not m.system_user 
    group by 
      1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19
    order by
      member_created_at desc, role desc
)
 , user_name as (
   select
   	user_uuid
   	, member_name
   from 
   	(
      select
      	user_uuid
      	, member_name
      	, row_number() over (partition by user_uuid order by member_created_at) as rn
      from user_details
      where not member_fivetran_deleted and not user_fivetran_deleted and active and accepted
    )
   where rn = 1
 )
 , account_primary_billing_contact as (
    select
      account_id as external_id
      , id as sf_contact_id
      , primary_contact_c
      , billing_contact_c
      , email as sf_email
      , row_number() over(partition by account_id order by created_date) as rn
    from
     {{ source('salesforce', 'contact') }}
    where
      not is_deleted
      and ( billing_contact_c or primary_contact_c )
      and not no_longer_with_company_c
    )

, user_status as (
    select
      ud.user_uuid
      --, ud.account_list  
      --, ud.account_name 
      --'[' ||  listagg(distinct case when not member_fivetran_deleted and not user_fivetran_deleted and active and accepted then '"' || ud.account_name || '"' end, ', ' ) within group(order by member_created_at) || ']' as account_name 
      , case
          when sum(case when not user_fivetran_deleted then 1 else 0 end) = 0 then 'Deleted'
          when sum(case when not user_fivetran_deleted and not member_fivetran_deleted then 1 else 0 end) = 0 then 'Terminated'
          when sum(case when not member_fivetran_deleted and not user_fivetran_deleted and active and accepted then 1 else 0 end ) > 0 then 'Active'
          when sum(case when not member_fivetran_deleted and not user_fivetran_deleted and active and not accepted then 1 else 0 end) >0 then 'Pending'
          else 'Terminated'
        end as status
    , min(case when not user_fivetran_deleted and not member_fivetran_deleted then member_created_at end) as member_created_at
        , '[' || listagg(case when not member_fivetran_deleted and not user_fivetran_deleted and active and accepted then '"' || ud.member_id || '"' end, ', ' ) within group(order by member_created_at) || ']' as member_id  
    , '[' ||  listagg(case when not member_fivetran_deleted and not user_fivetran_deleted and active and accepted then '"' || ud.organisation_id || '"' end, ', ' ) within group(order by member_created_at) || ']' as organisation_id
    , '[' ||  listagg(case when not member_fivetran_deleted and not user_fivetran_deleted and active and accepted then '"' || ud.role || '"' end, ', ' ) within group(order by member_created_at) || ']' as role
    , '[' ||  listagg(case when not member_fivetran_deleted and not user_fivetran_deleted and active and accepted then '"' || ud.job_title || '"' end, ', ' ) within group(order by member_created_at) || ']' as job_title  
    , listagg(distinct case when not member_fivetran_deleted and not user_fivetran_deleted and active and accepted then ud.account_email end, ', ' ) within group(order by member_created_at) as account_email  -- unique
    , listagg(distinct case when not member_fivetran_deleted and not user_fivetran_deleted and active and accepted then ud.company_email end, ', ' ) within group(order by member_created_at) as company_email  
    --, listagg(distinct case when not member_fivetran_deleted and not user_fivetran_deleted then ud.email end, ', ' ) within group(order by member_created_at) as email  
  , bool_or(case when not member_fivetran_deleted and not user_fivetran_deleted and active and accepted then ud.independent_contractor end) as is_contractor
  , max(case when not member_fivetran_deleted and not user_fivetran_deleted then ud.termination_date end) as termination_date
  , bool_or(case when not member_fivetran_deleted and not user_fivetran_deleted and active and accepted then coalesce(pbc.primary_contact_c, false) end) as sf_primary_contact
  , bool_or(case when not member_fivetran_deleted and not user_fivetran_deleted and active and accepted then coalesce(pbc.billing_contact_c, false) end) as sf_billing_contact
  , min(case when not member_fivetran_deleted and not user_fivetran_deleted and active and accepted and ud.role != 'employee' then ud.member_created_at else null::date end) as admin_created_at
  , bool_or(case when not member_fivetran_deleted and not user_fivetran_deleted and active and accepted and ud.active and ud.accepted and ud.role != 'employee' then true else false end) as current_admin 
    from user_details ud
    left join account_primary_billing_contact pbc on
    ud.account_list = pbc.external_id
    and ud.email = pbc.sf_email
    and pbc.rn = 1    
    group by user_uuid, user_fivetran_deleted
  )

  select
  us.user_uuid
  , case when us.company_email ilike '%@%' then split_part(us.company_email, ',', 1) else us.account_email end as email -- only picking the first created member company email  
  , us.account_email
  , d.account_list 
  , d.account_name
  , us.member_created_at
  , us.member_id   
  , un.member_name
  , us.organisation_id
  , us.role
  , us.job_title
  , us.status    
  , us.is_contractor
  , us.termination_date
  , us.sf_primary_contact
  , us.sf_billing_contact
  , us.admin_created_at
  , us.current_admin  
  , getdate() as _fivetran_transformed
from
  user_status as us 
  left join user_name as un on
  	us.user_uuid = un.user_uuid
  left join (select distinct user_uuid, account_list, account_name
             from user_details ) d on 
    d.user_uuid = us.user_uuid