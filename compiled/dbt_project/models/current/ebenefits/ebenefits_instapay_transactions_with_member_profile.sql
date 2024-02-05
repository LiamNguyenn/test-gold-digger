

with timesheets as (
  select 
  heropay_balance_id
  , sum(datediff('day', start_time, end_time)+ 1) as days
  , sum(units) as units
  from "dev"."heropay_db_public"."timesheets" 
  where not _fivetran_deleted
  group by 1 
  )

, payslips as (
  select id, member_id  
  , ps.created_at
  , ps.pay_period_starting
  , ps.pay_period_ending
  , round(wages/(datediff(day, ps.pay_period_starting, ps.pay_period_ending)+1)* 365, 0) as annum_wages
  --, ROW_NUMBER() OVER (PARTITION BY member_id, pay_period_ending order by created_at desc) as rn 
  from  "dev"."postgres_public"."payslips" ps
  where not ps._fivetran_deleted     
  --and ps.created_at > '2022-10-01'
  --order by member_id, pay_period_ending, rn
  )

, salaries as (
  select member_id   
  , effective_from  
  , case when salary_type ~* 'hour' and hours_per_week > 0 then salary * hours_per_week * 52
         when salary_type ~* 'day' and days_per_week > 0 then salary * days_per_week * 52
         when salary_type ~* 'fortnight' and days_per_week > 0 then salary * days_per_week * 26
         when salary_type ~* '^month' then salary * 12 
         when salary_type ~* 'annum' then salary
         else 0 end as salary
  , lead(effective_from, 1) over (PARTITION by member_id order by effective_from) as effective_to 
  from "dev"."postgres_public"."salary_versions" 
  where not _fivetran_deleted
  and salary > 0    
  and effective_from is not null
)
  
, instapay_members as (
  select distinct
  ht.id as transaction_id
  , ht.created_at as transaction_date  
  , m.id as member_id
  , m.user_id
  , m.organisation_id
  , datediff('year', m.date_of_birth, ht.created_at) as age
  , case when m.gender ~* '^f' then 'Female'
       when gender ~* '^m' then 'Male' else 'N/A' end as gender
  , case when age < 18 THEN ' <18'
      WHEN age >= 18 AND age <= 24 THEN '18-24'
      WHEN age > 24 AND age <= 34 THEN '25-34'
      WHEN age > 34 AND age <= 44 THEN '35-44'
      WHEN age > 44 AND age <= 54 THEN '45-54'
      WHEN age > 54 AND age <= 64 THEN '55-64'
      WHEN age > 64 THEN '65+' end as age_bracket 
  , (
        case
          when mi.salary_type = 'Annum' then  round(mi.salary, 0)
          when mi.salary_type = 'Hour' and ts.units is not null then round(mi.salary * ts.units / ts.days * 365, 0)
          else sum(ps.annum_wages) 
        end
      ) as annum_salary      
  , case when coalesce(annum_salary, s.salary)/1000 < 40 then '<40k'
      when coalesce(annum_salary, s.salary)/1000 >= 40 and coalesce(annum_salary, s.salary)/1000 < 50 then '40-50k'
      when coalesce(annum_salary, s.salary)/1000 >= 50 and coalesce(annum_salary, s.salary)/1000 < 70 then '50-70k'
      when coalesce(annum_salary, s.salary)/1000 >= 70 and coalesce(annum_salary, s.salary)/1000 < 100 then '70-100k'
      when coalesce(annum_salary, s.salary)/1000 >= 100 and coalesce(annum_salary, s.salary)/1000 < 120 then '100-120k' 
      when coalesce(annum_salary, s.salary)/1000 >= 120 and coalesce(annum_salary, s.salary)/1000 < 160 then '120-160k' 
      when coalesce(annum_salary, s.salary)/1000 >= 160 then '160k+' end as income_bracket
  , i.consolidated_industry as industry   
  , (
    case
      when mi.employment_type = 1
        then 'Full-time'
      when mi.employment_type = 2
        then 'Casual'
      when mi.employment_type = 0
        then 'Part-time'
    end
  ) as employment_type  
  , DATEDIFF(day, b.pay_period_starting, b.pay_period_ending) + 1 as pay_period  
  , (
    case when pay_period = 7
        then 'weekly'
      when pay_period = 14
        then 'fortnightly'
      when pay_period >= 28
        then 'monthly'
    end
  )  as pay_frequency  
  , case when os.active_employees < 20 then '1-19'
      when os.active_employees >= 20 and os.active_employees < 200 then '20-199' 
      when os.active_employees >= 200 then '200+' end as business_size
  , case
          when ea.state ~* '(South Australia|SA)' then 'SA'
          when ea.state ~* '(Northern Territory|NT)' then 'NT'
          when ea.state ~* '(Victoria|VIC)' then 'VIC'
          when ea.state ~* '(New South|NSW)' then 'NSW'
          when ea.state ~* '(Queensland|QLD)' then 'QLD'
          when ea.state ~* '(Tasmania|TAS)' then 'TAS'
          when ea.state ~* '(Western Australia|WA)' then 'WA'
          when ea.state ~* '(Australian Capital Territory|ACT)' then 'ACT'
          else null end as residential_state
from "dev"."heropay_db_public"."heropay_transactions" as ht
join "dev"."employment_hero"."employees" as m on m.uuid = ht.member_id
join "dev"."employment_hero"."_v_active_employees_by_organisations" os on m.organisation_id = os.organisation_id  
join "dev"."employment_hero"."organisations" o on m.organisation_id = o.id 
left join "dev"."heropay_db_public"."heropay_balances" b on b.id = ht.heropay_balance_id and not b._fivetran_deleted
left join "dev"."heropay_db_public"."member_infos" mi on mi.heropay_balance_id = b.id and not mi._fivetran_deleted
left join "dev"."one_platform"."industry" as i on regexp_replace( o.industry,'\\s','') = regexp_replace( i.eh_industry,'\\s','')
left join timesheets ts on ts.heropay_balance_id = ht.heropay_balance_id
left join salaries s on s.member_id = m.id and (s.effective_from <= ht.created_at) and (s.effective_to is null or s.effective_to > ht.created_at )
left join payslips ps on ps.pay_period_starting = b.pay_period_starting and ps.pay_period_ending = b.pay_period_ending and ps.member_id = m.id 
left join "dev"."postgres_public"."addresses" ea on m.address_id = ea.id and not ea._fivetran_deleted
where o.pricing_tier not ilike '%free%'


        and ht.created_at > (SELECT MAX(transaction_date) FROM "dev"."ebenefits"."instapay_transactions_with_member_profile" ) 
 
-- and ht.status = 1 --'payment_processed' and ht.created_at >= '2022-10-01'

  group by ht.id, ht.created_at, m.id, m.user_id, m.organisation_id, m.date_of_birth, mi.salary, i.consolidated_industry, mi.employment_type, mi.salary_type, mi.hours_per_week, b.pay_period_starting, b.pay_period_ending, m.gender, os.active_employees, ea.state, ts.units, ts.days, s.salary
  )

select * from instapay_members