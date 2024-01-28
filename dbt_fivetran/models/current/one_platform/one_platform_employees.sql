{{ config(alias='employees') }}

with valid_eh_members as (
  select m.id    
    , m.email
    , m.first_name
    , m.last_name
    , case when regexp_replace(company_mobile, '[^0-9+]+', '') is not null and regexp_replace(company_mobile, '[^0-9+]+', '') != '' then regexp_replace(company_mobile, '[^0-9+]+', '')
        else regexp_replace(personal_mobile_number, '[^0-9+]+', '') end as mobile_phone       
    , m.organisation_id
    , m.created_at
    , m.work_country
    , case when m.termination_date < getdate() then false else m.active end as active 
    , m.date_of_birth
    , m.gender
    , o.eh_sub_name
    , o.eh_pricing_tier
    , case when not m.active then false else o.is_paying_eh end as is_paying_eh
    , o.eh_churn_date
    , m.start_date
    , case when o.eh_sub_name ilike '%churn%' or not m.active then coalesce(least(m.termination_date, o.eh_churn_date), m.created_at) 
        else m.termination_date end as termination_date
    , m.user_id
    , m.user_uuid
    , m.external_id
    , o.kp_business_id
  from {{ref('employment_hero_employees')}} as m
  left join {{ref('one_platform_organisations')}} as o on m.organisation_id = o.eh_organisation_id    
  where (m.termination_date is null or m.termination_date >= m.created_at)
)

, valid_kp_employees as (
  select e.id::integer
    , e.email
    , e.firstname as first_name
    , e.surname as last_name
    , regexp_replace(e.mobile_phone, '[^0-9+]+', '') as mobile_phone
    , e.business_id
    , e.date_created     
    , cl.country as work_country
    , e.date_of_birth
    , case when e.gender = 'F' then 'Female' when e.gender = 'M' then 'Male' else e.gender end as gender
    , case when (e.start_date <= getdate() or e.start_date is null) and (e.end_date is null or e.end_date > getdate()) then true else false end as active
    , least(e.start_date, bl.first_billing_month) as start_date
    , e.end_date
    , bl.last_billing_month::date
    , listagg(distinct ue.user_id, ', ') as user_ids    
  from {{ref('keypay_dwh_employee')}}  e 
  join {{ref('keypay_business_traits')}} b on e.business_id = b.id 
  left join {{ref('keypay_dwh_suburb')}} s on e.residential_suburb_id = s.id  
  left join {{source('csv', 'country_geo_location')}} cl on s.country = cl.name
  left join  {{ref('keypay_user_employee')}} ue on ue.employee_id = e.id 
  left join (
      select employee_id, max(billing_month::date) as last_billing_month, min(billing_month::date) as first_billing_month from {{ref('keypay_t_pay_run_total_monthly_summary')}} group by 1
  )bl on bl.employee_id = e.id 
  where (e.end_date is null or e.end_date >= e.date_created) 
  group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14
)

  select 
    CONCAT(case when m.id is not null then 'EH-' || m.id else '' end, case when m.id is not null and k.id is not null then '_KP-' || k.id when k.id is not null then 'KP-' || k.id else '' end) as omop_emp_id
    , m.id as eh_member_id
    , k.id as kp_employee_id  
    , coalesce(m.email, k.email) as email
    , coalesce(m.first_name, k.first_name) as first_name
    , coalesce(m.last_name, k.last_name) as last_name
    , coalesce(m.date_of_birth, k.date_of_birth) as date_of_birth
    , coalesce(m.gender, k.gender) as gender
    , coalesce(m.mobile_phone, k.mobile_phone) as mobile_phone
    , m.organisation_id as eh_organisation_id
    , k.business_id as kp_business_id
    , coalesce(m.work_country, k.work_country) as work_country
    , least(m.created_at, k.date_created) as created_at
    , case when m.active or k.active then true else false end as active
    , least(m.start_date, k.start_date) as start_date
    , case when ((m.termination_date is null and m.active) or (k.end_date is null and k.active)) then null else greatest(m.termination_date, k.end_date) end as termination_date
    , m.eh_sub_name
    , m.eh_pricing_tier
    , m.is_paying_eh
    , m.eh_churn_date
    , k.last_billing_month as kp_last_billing_month
    , m.user_id as eh_user_id
    , m.user_uuid as eh_user_uuid
    , k.user_ids as kp_user_ids      
  from valid_eh_members m    
    full outer join valid_kp_employees k on m.external_id = k.id and m.kp_business_id is not null