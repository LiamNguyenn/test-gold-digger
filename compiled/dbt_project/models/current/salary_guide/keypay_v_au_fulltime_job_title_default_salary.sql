with
 recent_payrun as (
  select      
      business_id
      , employee_id
      , employment_type
      , residential_state
      , gender
      , monthly_gross_earnings
      , total_hours
      , hourly_rate
      , billing_month
      , row_number() over (partition by business_id, employee_id order by billing_month desc) as billing_month_desc
      --, case when payrun_monthly_hours = 0 then null else payrun_monthly_earnings/payrun_monthly_hours end as payrun_hourly_rate
    from "dev"."keypay"."au_pay_run_summary_s"    
    where
      --employment_type = 'Full-time' and
      billing_month < DATE_TRUNC('month', CURRENT_DATE)
      and billing_month >= DATE_TRUNC('month', dateadd('month', -12, CURRENT_DATE))
      and hourly_rate > 0
)

, employee_pay_category as (
  select    
   pd.job_title, pd.business_id
   , cu.description as pay_category_rate_unit, eu.description as employee_rate_unit
   , c.* 
   , pr.residential_state
   , pr.gender      
   , pr.monthly_gross_earnings
   , pr.total_hours   
   , pr.billing_month
  from -- latest default pay
    (
      select * from "dev"."keypay"."employee_pay_category"
        where id in
        (
              select
                FIRST_VALUE(id) over (
                  partition by 
                    employee_id
                    order by to_date::date desc nulls first, from_date::date desc
                    rows between unbounded
                    preceding and unbounded following
                 )
              from "dev"."keypay"."employee_pay_category" c
            where (c.from_date::date < getdate())
              and (c.to_date::date is null or c.to_date = 'NULL' or c.to_date::date > DATE_TRUNC('month', dateadd('month', -13, getdate()))) 
              and (c.expiry_date::date is null or c.expiry_date = 'NULL' or c.expiry_date::date > DATE_TRUNC('month', dateadd('month', -13, CURRENT_DATE))) 
              and c.is_default  -- multiple default pay categories 
              and (standard_weekly_hours > 0 or standard_daily_hours > 0)
          )         
     )c
  join "dev"."keypay"."rate_unit" cu on c.pay_category_rate_unit_id = cu.id 
  join "dev"."keypay"."rate_unit" eu on c.employee_rate_unit_id = eu.id    
  join "dev"."keypay_dwh"."employee" e on c.employee_id = e.id 
  join recent_payrun pr on pr.employee_id = c.employee_id and billing_month_desc = 1  
   join "dev"."keypay"."payrun_default" pd on e.pay_run_default_id = pd.id and pd.employee_id = e.id 
where --(e.start_date < getdate()) and (e.end_date is null or e.end_date > getdate())  -- having employee_pay_category: 210019   and
  cu.description != 'Fixed'  --??? ignore this type, 195667
  and pd.from_date::date < CURRENT_DATE
  and (pd.to_date::date is null or pd.to_date::date > DATE_TRUNC('month', dateadd('month', -13, CURRENT_DATE))) 
  and pd.job_title is not null and pd.job_title !~ '^$' and len(pd.job_title) > 1
)

, employee_latest_salary as (
  select 
  business_id
    , employee_id
    , billing_month as last_billing_month
    , residential_state
    , gender
    , job_title    
    , sum(estimated_pay) as estimated_annual_pay   
    , sum(standard_weekly_hours) as weekly_hours
    , sum(default_annual_pay) as annual_pay
    from (
      select *
        , case when pc.pay_category_rate_unit = 'Annually' then pc.calculated_rate
            when pc.pay_category_rate_unit = 'Hourly' then pc.calculated_rate * pc.standard_weekly_hours * 365/7 
            when pc.pay_category_rate_unit = 'Monthly' then pc.calculated_rate * 12
            --when pc.pay_category_rate_unit = 'Fixed' then pc.calculated_rate * pc.standard_weekly_hours * 365/7  -- ??? ignore this 
            when pc.pay_category_rate_unit = 'Daily' then pc.calculated_rate * pc.standard_weekly_hours * 365/7
        end as default_annual_pay       
      , case when pc.total_hours = 0 then null else pc.monthly_gross_earnings/pc.total_hours * pc.standard_weekly_hours * 365/7 end as estimated_pay 
      from employee_pay_category pc
      )
    group by 1,2,3,4,5,6
  -- filter out the suspicious records with much higher annual pay than pay run. 
  having weekly_hours > 30  -- full time
  and annual_pay > 0 
  and case when estimated_annual_pay > 0 then abs(annual_pay - estimated_annual_pay) < 0.5 * annual_pay else true end
  )

, t_cleansed as (
    select job_title,  
-- remove ending words   
trim(regexp_replace(trim(regexp_replace(trim(regexp_replace(trim(regexp_replace(         
    trim(replace(TRIM(REGEXP_REPLACE(REGEXP_REPLACE(replace(trim(lower(
        -- abbreviations
        trim(job_title_abbreviation_expand( 
            -- replace & with and
            trim(replace(replace(
                -- replace + with and
                trim(replace(replace(
                    -- 5. replace & with and
                    trim(replace(replace(
                        -- 4. replace ! with of
                        trim(replace(replace(replace(replace(replace(replace(replace(
                            -- 3. trim ending special characters
                            trim(trim('&' from trim(trim('/' from trim(trim(':' from trim(trim('|' from trim(trim('-' from trim(trim('|' FROM ( 
                                -- 2. remove state
                                trim(REGEXP_REPLACE(TRIM(REGEXP_REPLACE(REGEXP_REPLACE(lower(   
                                    -- 1. remove content inside bracket
                                    trim(REGEXP_REPLACE(job_title, '\\([^)]*\\)'))
                                ), '(^|\\W)(act|nsw|nt|qld|sa|tas|vic|wa|new south wales|victoria|queensland|western australia|south australia|tasmania|australian capital territory|northern territory|brisbane|canberra|darwin|hobart|melbourne|perth|sydney)(\\W|$)', ' '), '(^|\\W)(act|nsw|nt|qld|sa|tas|vic|wa|new south wales|victoria|queensland|western australia|south australia|tasmania|australian capital territory|northern territory|brisbane|canberra|darwin|hobart|melbourne|perth|sydney)(\\W|$)', ' ')), '-$'))
                            )))))))))))))
                        , ' - ', ' of '), ' : ', ' of '), ':', ' of '), ' | ', ' of '), '|', ' of '), ', ', ' of '), ',', ' of '))
                    , ' / ', ' and '), '/', ' and '))
                , ' + ', ' and '), '+', 'and'))
            , ' & ', ' and '), '&', ' and '))
        ))
    )), ' the ', ' '), '^[-/]', ''), '[-/]$', '')), '  ', ' '))
, '( of| to| \or| \and)$', '')), '( of| to| \or| \and)$', '')), '( of| to| \or| \and)$', '')), '( of| to| \or| \and)$', ''))
 as t_title,
    business_id, employee_id, residential_state, gender, annual_pay, last_billing_month
    from employee_latest_salary
)

, t_common as (
    select t.job_title, trim(INITCAP(coalesce(m.title_common, t.t_title))) as t_title,
    business_id, employee_id, residential_state, gender, annual_pay, last_billing_month
    from t_cleansed t 
    left join "dev"."csv"."more_common_job_titles" m on t.t_title = m.title_original
)
 
, business_eh_industry as (
    select
      m.id as business_id
      , case 
          when m.industry = 'Other' then 'Other'
          when m.industry != 'Other' and m.industry is not null then i.matched_eh_industry 
          else null
        end as industry
    from 
      (
        select
          b.id
          , case
              when b.industry_id is null and b.industry_name is not null 
                then 'Other'
              when b.industry_id is null and b.industry_name is null 
                and z.primary_industry is not null and z.primary_industry != '' 
                then z.primary_industry
              when b.industry_id is not null then i.name             
              else null
            end as industry
        from
          "dev"."keypay_dwh"."business" as b
          left join (select id, name from "dev"."keypay"."industry") as i on
            b.industry_id = i.id
          left join (select _id, primary_industry from "dev"."keypay"."zoom_info") as z on
            b.id = z._id
      ) as m
    left join "dev"."one_platform"."industry" as i on
      regexp_replace( m.industry,'\\s','') = regexp_replace( i.keypay_industry,'\\s','')
      or regexp_replace( m.industry,'\\s','') = regexp_replace( i.zoom_info_primary_industry,'\\s','')
      or regexp_replace( m.industry,'\\s','') = regexp_replace( i.eh_industry,'\\s','')
  )
                              
select 
    t.job_title
    , t.t_title as processed_title
 	, t.business_id as organisation_id
    , t.employee_id as member_id
    , t.residential_state
    , t.annual_pay as annual_salary
    , last_billing_month
    , i.industry
from t_common t 
join business_eh_industry i on t.business_id = i.business_id