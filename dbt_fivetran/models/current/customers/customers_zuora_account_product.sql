{{ config(alias='zuora_account_product') }}

with snapshot_billed_employees as (
select
  o.id as organisation_id
  , count(*) as billed_users
  , count(case when independent_contractor then 1 else null end) as contractors
  from
  {{ source('postgres_public', 'billed_employees') }} be
  join {{ source('postgres_public', 'billable_employee_snapshots') }} bes on
    be.billable_employee_snapshot_id = bes.id
  join {{ source('postgres_public', 'organisations') }} as o on
    o.id = bes.organisation_id
where
  not be._fivetran_deleted
  and not bes._fivetran_deleted
  and not o._fivetran_deleted
  and not o.is_shadow_data
  and snapshot_date < DATE_TRUNC('month', getdate()) and snapshot_date > dateadd(month, -1, DATE_TRUNC('month', getdate()))  
  group by 1
  )

,  orgs_per_zuora_subscription as (
    select
      s.account_id as zuora_account_id
      , s.id as zuora_subscription_id
      , count(distinct zs.organisation_id) as num_of_orgs
      , sum(sbe.billed_users) as billed_users
      , sum(sbe.contractors) as contractors
    from 
      {{ source('postgres_public', 'zuora_subscriptions') }} zs 
      join {{ source('zuora', 'subscription') }} s on zs.zuora_subscription_number = s.name
      left join 
          (
          select *
            from {{ source('postgres_public', 'agreements') }}
            where id in (
              select
                FIRST_VALUE(id) over (
                  partition by 
                    organisation_id
                    order by created_at desc 
                    rows between unbounded
                    preceding and current row
                 )
              from
                {{ source('postgres_public', 'agreements') }}
              where not _fivetran_deleted
            )
        )a
        on zs.organisation_id = a.organisation_id
      --left join [sub_plan_grouping as spg] on a.subscription_plan_id = spg.id
      left join snapshot_billed_employees sbe on zs.organisation_id = sbe.organisation_id
    where 
      not s._fivetran_deleted
      and not zs._fivetran_deleted
      --and pricing_tier !~ 'free'
      and a.subscription_plan_id not in (
        4, -- Startup Premium
        7,	-- Free (30 days)
        11,	-- Free
        17, -- Demo
        43, -- CHURN (FREE)
        52, -- Implementations Free
        53, -- Startup Standard
        55, -- ANZ Free
        144, -- International Free
        145, -- Premium Trial
        161, -- SUSPENDED (FREE) 
        162 -- SEA free
      )
    group by 1,2
  )

  , account_current_product as (
    select
    a.id as zuora_account_id
    , a.account_number as zuora_account_num
    , a.name as zuora_account_name
    , s.id as subscription_id
    , s.name as subscription_num
    , a.crm_id as salesforce_id
--     , a.batch
    , p.id as product_id
    , p.name as product_name
    , prp.name  as product_rate_plan
    , prp.id  as product_rate_plan_id
    , rpc.mrr as product_mrr
    , rpc.quantity as contracted_users
    , ozs.num_of_orgs
--     , rpc.effective_start_date
--     , rpc.effective_end_date
--     , rpc.name as charge_name
  from
    {{ source('zuora', 'account') }} a
    join {{ source('zuora', 'subscription') }} s on
        a.id = s.account_id 
    join {{ source('zuora', 'rate_plan_charge') }} rpc on
      rpc.subscription_id = s.id
    join {{ source('zuora', 'product_rate_plan') }} prp on
      rpc.product_rate_plan_id = prp.id
    join {{ source('zuora', 'product') }} p on
      p.id = prp.product_id
    left join orgs_per_zuora_subscription ozs on
      s.id = ozs.zuora_subscription_id
  where
    not a._fivetran_deleted
    and not s._fivetran_deleted
    and not p._fivetran_deleted
    and not prp._fivetran_deleted
    and not rpc._fivetran_deleted
    and a.batch != 'Batch50'
    and s.status != 'Expired'
    and s.status != 'Cancelled'
--     and p.name in ('EH HR Software','EH Payroll Software')
    and rpc.name in ('Contracted Users','Contracted Employees')
--     and prp.locale_c = 'AU'
    and p.name not in ('Services', 'Discounts')
    and a.geo_code_c = 'AU'
    and rpc.effective_start_date <= getdate()
    and (rpc.effective_end_date is null or rpc.effective_end_date > getdate())
  )

  , account_latest_invoice as (
    -- to get the latest billed users per account, check latest invoice
    -- recent accounts or accounts that changed tiers will not have recent invoices relating to their product so just use the contracted users calculated from previous CTE (coalese). same story with mrr
    select * 
    from 
      (
        select 
          invoice.account_id as zuora_account_id
          , acp.zuora_account_num
          , acp.zuora_account_name
          , invoice_item.subscription_id
          , acp.product_name
          , acp.product_rate_plan_id
          , acp.product_rate_plan
          , invoice.invoice_number
          , invoice_item.charge_name
          , invoice_item.quantity as billed_users
          , invoice_item.charge_amount
--           , (invoice_item.unit_price + invoice_item.tax_amount) as price_incl_tax
--           , invoice_item.charge_amount + invoice_item.tax_amount as charge_amount_incl_tax
          , invoice.posted_date
--           , rank() over(partition by invoice.account_id order by invoice.posted_date desc) rk
          , rank() over(partition by invoice.account_id order by date_trunc('month', invoice.posted_date) desc) rk
        from {{ source('zuora', 'invoice') }}
          join {{ source('zuora', 'invoice_item') }} on invoice.id = invoice_item.invoice_id
          join account_current_product acp on invoice_item.product_rate_plan_id = acp.product_rate_plan_id and acp.zuora_account_id = invoice.account_id
        
        where 
          not invoice._fivetran_deleted                       
          and not invoice_item._fivetran_deleted
          and invoice.status = 'Posted'              
          and invoice.posted_date <= getdate()            
          and invoice_item.charge_amount > 0
          and charge_name not ilike '%sms%'
      )
    where rk = 1
  )

, global_teams as (     
    select 
      za.account_number as zuora_account_num                  
      , sp.name as product
      , sum(so.opportunity_employees_c) as global_teams_opp_users
    from {{ source('zuora', 'account') }} za 
      join {{ source('salesforce', 'account') }} sa on za.crm_id = sa.id
      join {{ source('salesforce', 'opportunity') }} so on so.account_id = sa.id
      left join {{ source('salesforce', 'opportunity_line_item') }} ol on so.id = ol.opportunity_id and not ol.is_deleted
      left join {{ source('salesforce', 'product_2') }} sp on ol.product_2_id = sp.id and not sp.is_deleted and sp.revenue_type_c != 'One-Off'
    where not za._fivetran_deleted 
      and not sa.is_deleted
      and not so.is_deleted
      and not ol.is_deleted
      and not sp.is_deleted
      and so.is_closed 
      and so.stage_name = 'Won' 
      and sp.name ilike '%Global Teams%'
  group by 1,2
      )

  , current_account_details as (
    select
      a.zuora_account_id
      , a.zuora_account_num
      , a.salesforce_id
      , a.zuora_account_name 
      , coalesce(sa.industry_primary_c, 'Unknown') as salesforce_industry
      , a.product      
      , a.num_of_orgs
      , a.contracted_mrr
      , a.contracted_users
      , coalesce(sum(i.billed_users), a.contracted_users) as recent_billed_users
      , coalesce(sum(i.charge_amount), a.contracted_mrr) as current_mrr        
    from
      (
        select 
          zuora_account_id
          , zuora_account_num
          , zuora_account_name
          , salesforce_id
          , product_rate_plan_id
          , product_name
          , (
            case
              when product_name = 'EH HR Software' and product_rate_plan ilike '%Platinum%'
                then 'HR Platinum'
              when product_name = 'EH HR Software' and product_rate_plan ilike '%Standard%'
                then 'HR Standard'
              when product_name = 'EH HR Software' and product_rate_plan ilike '%Premium%'
                then 'HR Premium'
              when product_name = 'EH HR Software' and product_rate_plan ilike '%Legacy%'
                then 'HR Legacy'
              when product_name = 'EH Payroll Software' and product_rate_plan ilike '%Standard%'
                then 'Payroll Standard'
              when product_name = 'EH Payroll Software' and product_rate_plan ilike '%Premium%'
                then 'Payroll Premium'
              else replace(product_rate_plan,' (Monthly)', '')
            end
            ) as product
          , sum(product_mrr) as contracted_mrr
          , sum(contracted_users) as contracted_users
          , sum(num_of_orgs) as num_of_orgs
        from account_current_product
        group by 1,2,3,4,5,6,7
      ) as a      
      left join account_latest_invoice i
        on a.zuora_account_id = i.zuora_account_id and a.product_rate_plan_id = i.product_rate_plan_id
      left join {{ source('salesforce', 'account') }} sa 
        on a.salesforce_id = sa.id
    group by 1,2,3,4,5,6,7,8,9
  )

-- TBD: may be billed elsewhere?
, accounts_no_users_and_mrr as (
    -- dead in the water accounts, still having active zuora subscriptions and still recieving invoices but have zero usage (users)
    -- needs to be excluded from current total and potential calculations
    select 
      zuora_account_num
    from
      (select
        zuora_account_num
        ,sum(contracted_users) as contracted_users
        ,sum(contracted_mrr) as contracted_mrr
        ,sum(recent_billed_users) as recent_billed_users
        ,sum(current_mrr) as current_mrr
      from 
      current_account_details
      group by 1)
    where 
      contracted_users= 0 
      and contracted_mrr= 0 
      and recent_billed_users=0 
      and current_mrr is null
  )

  select 
      cad.zuora_account_id
      , cad.zuora_account_num
      , cad.salesforce_id
      , cad.zuora_account_name
      , cad.salesforce_industry
      , agt.GT1_opp_users
      , agt.GT2_opp_users
      , agt.GT4_opp_users

      , sum(case when cad.product = 'HR Legacy' then cad.recent_billed_users end) hr_legacy_recent_billed_users
      , sum(case when cad.product = 'HR Legacy' then ozs.eh_contractors end) hr_legacy_contractors
--       , sum(case when cad.product = 'HR Legacy' then cad.num_of_orgs end) hr_legacy_num_of_orgs      
      , sum(case when cad.product = 'HR Standard' then cad.recent_billed_users end) hr_standard_recent_billed_users
      , sum(case when cad.product = 'HR Standard' then ozs.eh_contractors end) hr_standard_contractors
--       , sum(case when cad.product = 'HR Standard' then cad.num_of_orgs end) hr_standard_num_of_orgs      
      , sum(case when cad.product = 'HR Premium' then cad.recent_billed_users end) hr_premium_recent_billed_users
      , sum(case when cad.product = 'HR Premium' then ozs.eh_contractors end) hr_premium_contractors
--       , sum(case when cad.product = 'HR Premium' then cad.num_of_orgs end) hr_premium_num_of_orgs    
      , sum(case when cad.product = 'HR Platinum' then cad.recent_billed_users end) hr_platinum_recent_billed_users
      , sum(case when cad.product = 'HR Platinum' then ozs.eh_contractors end) hr_platinum_contractors
--       , sum(case when cad.product = 'HR Platinum' then cad.num_of_orgs end) hr_platinum_num_of_orgs      
      , sum(case when cad.product = 'Payroll Standard' then cad.recent_billed_users end) payroll_standard_recent_billed_users
--       , sum(case when cad.product = 'Payroll Standard' then cad.num_of_orgs end) payroll_standard_num_of_orgs     
      , sum(case when cad.product = 'Payroll Premium' then cad.recent_billed_users end) payroll_premium_recent_billed_users
--       , sum(case when cad.product = 'Payroll Premium' then cad.num_of_orgs end) payroll_premium_num_of_orgs      
      , sum(case when cad.product = 'EAP Standard' then cad.recent_billed_users end) eap_standard_recent_billed_users
--       , sum(case when cad.product = 'EAP Standard' then cad.num_of_orgs end) eap_standard_num_of_orgs      
      , sum(case when cad.product = 'EAP Premium' then cad.recent_billed_users end) eap_premium_recent_billed_users
--       , sum(case when cad.product = 'EAP Premium' then cad.num_of_orgs end) eap_premium_num_of_orgs      
      , sum(case when cad.product = 'HR Advisor' then cad.recent_billed_users end) hr_advisor_recent_billed_users
--       , sum(case when cad.product = 'HR Advisor' then cad.num_of_orgs end) hr_advisor_num_of_orgs    
      , sum(case when cad.product = 'Payroll Advisor' then cad.recent_billed_users end) payroll_advisor_recent_billed_users
--       , sum(case when cad.product = 'Payroll Advisor' then cad.num_of_orgs end) payroll_advisor_num_of_orgs     
      , sum(case when cad.product = 'Phone Technical Support' then cad.recent_billed_users end) phone_technical_support_recent_billed_users
--       , sum(case when cad.product = 'Phone Technical Support' then cad.num_of_orgs end) phone_technical_support_num_of_orgs     
      , sum(case when cad.product = 'Ultimate Support' then cad.recent_billed_users end) ultimate_support_recent_billed_users
--       , sum(case when cad.product = 'Ultimate Support' then cad.num_of_orgs end) ultimate_support_num_of_orgs     
      , sum(case when cad.product = 'LMS Plus' then cad.recent_billed_users end) lms_plus_recent_billed_users
--       , sum(case when cad.product = 'LMS Plus' then num_of_orgs end) lms_plus_num_of_orgs


      , sum(case when cad.product = 'HR Legacy' then cad.current_mrr end) hr_legacy_current_mrr
      , sum(case when cad.product = 'HR Standard' then cad.current_mrr end) hr_standard_current_mrr
      , sum(case when cad.product = 'HR Premium' then cad.current_mrr end) hr_premium_current_mrr
      , sum(case when cad.product = 'HR Platinum' then cad.current_mrr end) hr_platinum_current_mrr
      , sum(case when cad.product = 'Payroll Standard' then cad.current_mrr end) payroll_standard_current_mrr
      , sum(case when cad.product = 'Payroll Premium' then cad.current_mrr end) payroll_premium_current_mrr
      , sum(case when cad.product = 'EAP Standard' then cad.current_mrr end) eap_standard_current_mrr
      , sum(case when cad.product = 'EAP Premium' then cad.current_mrr end) eap_premium_current_mrr
      , sum(case when cad.product = 'HR Advisor' then cad.current_mrr end) hr_advisor_current_mrr
      , sum(case when cad.product = 'Payroll Advisor' then cad.current_mrr end) payroll_advisor_current_mrr
      , sum(case when cad.product = 'Phone Technical Support' then cad.current_mrr end) phone_technical_support_current_mrr 
      , sum(case when cad.product = 'Ultimate Support' then cad.current_mrr end) ultimate_support_current_mrr
      , sum(case when cad.product = 'LMS Plus' then cad.current_mrr end) lms_plus_current_mrr  
    from 
    current_account_details cad
    left join (
      select zuora_account_id, sum(contractors) as eh_contractors 
      from orgs_per_zuora_subscription group by 1) ozs on ozs.zuora_account_id = cad.zuora_account_id
    left join (
      select zuora_account_num
        , sum(case when gt.product = 'Global Teams (PEO) - Tier 1' then gt.global_teams_opp_users end) GT1_opp_users
        , sum(case when gt.product = 'Global Teams (PEO) - Tier 2' then gt.global_teams_opp_users end) GT2_opp_users
        , sum(case when gt.product = 'Global Teams (PEO) - Tier 4' then gt.global_teams_opp_users end) GT4_opp_users
      from global_teams gt group by 1)agt on agt.zuora_account_num = cad.zuora_account_num
--where cad.zuora_account_num not in (select zuora_account_num from accounts_no_users_and_mrr
     group by 1,2,3,4,5,6,7,8