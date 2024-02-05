

with 
  stage_churn as (
  -- view with churn account details, ie.is_churn=0 means account churned
  select
    za.id     
    , count(
      case
        when(
          zs.status = 'Active'
          or zs.status = 'Suspended'
        )
         then 1
        else null
      end
    ) as is_churn
  from
    "dev"."zuora"."account" za 
    inner join "dev"."zuora"."subscription" zs on zs.account_id = za.id
    left join "dev"."zuora"."rate_plan_charge" zrpc on zs.id = zrpc.subscription_id
    left join "dev"."zuora"."product_rate_plan" zprp on zrpc.product_rate_plan_id = zprp.id
    left join "dev"."zuora"."product" zp on zprp.product_id = zp.id
  where
    not za._fivetran_deleted
    and not zs._fivetran_deleted
    and not zp._fivetran_deleted
    and not zprp._fivetran_deleted
    and not zrpc._fivetran_deleted
    --and zp.name ilike '%hr software%'
  group by 1
)
, org_creators as (
  select
      organisation_id
      , member_id
      , user_email
    from
      (
        select
        m.organisation_id
        , m.id as member_id
        , u.email as user_email
        , row_number() over (partition by m.organisation_id order by m.created_at) as rn
        from
          "dev"."postgres_public"."members" as m
        join "dev"."postgres_public"."users" as u on
          m.user_id = u.id
        where
          not m._fivetran_deleted        
          and not u._fivetran_deleted
          and not m.is_shadow_data
          and not u.is_shadow_data
        )
    where rn = 1
)
, churn_date_field as (
    -- view with churn account date
    select
      scn.id
      --, sa.churn_date_c, sa.downgrade_to_churn_date_c, zs.subscription_end_date, zs.cancelled_date, zs.term_end_date, sa.churn_request_date_c      
      , max(coalesce(zs.cancelled_date, zs.subscription_end_date, zs.term_end_date)) as churn_date 
    from
      stage_churn as scn            
      inner join "dev"."zuora"."subscription" zs on
        zs.account_id = scn.id
    left join "dev"."zuora"."rate_plan_charge" zrpc on zs.id = zrpc.subscription_id
    left join "dev"."zuora"."product_rate_plan" zprp on zrpc.product_rate_plan_id = zprp.id
    left join "dev"."zuora"."product" zp on zprp.product_id = zp.id
      inner join  (
        -- get the last version of a subscription
        select s.account_id
      , s.name as sub_name
      , max(s.version) as version
    from
      "dev"."zuora"."subscription" s
      join "dev"."zuora"."account" a on
        s.account_id = a.id
    where
      not a._fivetran_deleted
      and not s._fivetran_deleted   
    group by
      1,2
    )cs on cs.sub_name = zs.name and zs.account_id = cs.account_id and cs.version = zs.version    
    where
      scn.is_churn = 0      
      and not zs._fivetran_deleted    
        and not zrpc._fivetran_deleted
        and not zprp._fivetran_deleted
        and not zp._fivetran_deleted
        --and zp.name ilike '%hr software%'
    group by 1
  )  

select 
  o.*,
--   addresses.country,
--   addresses.state,
--   addresses.city as suburb,
--   addresses.postcode,
  i."title" as industry,
  trim('Auth' from p.type) as payroll_type, 
  p.connected_app, 
  s.id as sub_id, 
  s.name as sub_name, 
  s.yin_yang,
  s.pricing_type,
  s.pricing_tier,
  s.pricing_hierarchy,
  sa.account_id
  , case when sc.id is not null and za.batch is not null and za.batch != 'Batch50' and (sc.is_churn > 0 or getdate() < cdf.churn_date or cdf.churn_date is null) and s.pricing_tier not ilike '%free%' then true    
    --when o.id in (select host_organisation_id from "dev"."employment_hero"."_v_gt_active_organisations") then true 
    else false end as is_paying_eh
  , coalesce(cdf.churn_date, case when a.subscription_plan_id = 43 and not a.cancelled then a.created_at end) as churn_date,
  oc.member_id as creator_member_id,
  oc.user_email as creator_email
from 
  "dev"."postgres_public"."organisations" as o
  left join 
    (
    select 
      *
    from "dev"."postgres_public"."agreements"
    where id in 
      (
      select
        FIRST_VALUE(id) over (partition by organisation_id order by created_at desc rows between unbounded preceding and current row)
      from
        "dev"."postgres_public"."agreements"
      where
        not cancelled
      )
    ) as a on o.id = a.organisation_id
  left join "dev"."employment_hero"."_v_sub_plan_grouping" as s
    on a.subscription_plan_id = s.id
  left join "dev"."employment_hero"."_v_last_connected_payroll" as p
    on o.id = p.organisation_id
  left join "dev"."postgres_public"."industry_categories" as i
    on o.industry_category_id = i.id
      and not i._fivetran_deleted
  left join "dev"."postgres_public"."salesforce_accounts" as sa
    on o.id = sa.organisation_id
      and not sa._fivetran_deleted
  left join "dev"."zuora"."account" za on o.zuora_account_id = za.id and not za._fivetran_deleted   
  left join stage_churn sc on sc.id = za.id
  left join churn_date_field cdf on cdf.id = za.id
  left join org_creators oc on o.id = oc.organisation_id
where 
  not o._fivetran_deleted
  and not o.is_shadow_data