-- the employee may go through several job titles within an org in 12 months. 
{{ config(materialized='table', enabled=false) }}

with dates as (
    select
      DATEADD('month', -1 - generated_number::int, (date_trunc('month', add_months(CURRENT_DATE, 1))))::date date
    from ({{ dbt_utils.generate_series(upper_bound=300) }})
    where date >= DATE_TRUNC('month', dateadd('month', -13, current_date))::date
  )

, recent_payrun as (
  select      
      business_id
      , employee_id
      , industry
      , residential_state      
      , row_number() over (partition by employee_id, business_id order by billing_month::date desc) as rn 
    from {{ref('keypay_au_pay_run_summary_s') }} 
    where
      business_id is not null
      and employee_id is not null
      and billing_month::date >= DATE_TRUNC('month', dateadd('month', -12, current_date))::date
        and billing_month::date  < DATE_TRUNC('month', CURRENT_DATE)::date -- this month not complete  
      and employment_type='Full-time'
)

, monthly_pay as (
  select d.date as month
  , coalesce(prt.employee_id, prth.employee_id) as employee_id
  , e.business_id
  , pd.job_title
  , rpr.industry
  , rpr.residential_state  
  , sum(coalesce(prt.gross_earnings, prth.gross_earnings)*(datediff(day, case when d.date <= pr.pay_period_starting::date then pr.pay_period_starting::date else d.date end, case when DATEADD('month', 1, d.date) > pr.pay_period_ending::date then pr.pay_period_ending::date else DATEADD('month', 1, d.date) end)+1)/(datediff(day, pr.pay_period_starting::date, pr.pay_period_ending::date)+1)) as monthly_wages  
  , sum(coalesce(prt.total_hours, prth.total_hours)*(datediff(day, case when d.date <= pr.pay_period_starting::date then pr.pay_period_starting::date else d.date end, case when DATEADD('month', 1, d.date) > pr.pay_period_ending::date then pr.pay_period_ending::date else DATEADD('month', 1, d.date) end)+1)/(datediff(day, pr.pay_period_starting::date, pr.pay_period_ending::date)+1)) as monthly_hours  
  from dates as d 
  join {{ ref('keypay_payrun') }} pr on pr.pay_period_starting::date < DATEADD('month', 1, d.date) and d.date <= pr.pay_period_ending::date
  join {{ ref('keypay_dwh_business') }} b on pr.business_id = b.id  
  left join {{ ref('keypay_payrun_total') }} prt on prt.payrun_id = pr.id and SPLIT_PART(pr._file, 'Shard', 2) = SPLIT_PART(prt._file, 'Shard', 2) and pr.date_first_finalised::date >= '2022-01-01' and prt.is_excluded_from_billing = 0 
    left join {{ source('keypay', 'payrun_total_history') }} prth on prth.payrun_id = pr.id and SPLIT_PART(pr._file, 'Shard', 2) = SPLIT_PART(prth._file, 'Shard', 2) and pr.date_first_finalised::date < '2022-01-01' and prth.is_excluded_from_billing = 0     
    join {{ ref('keypay_dwh_employee') }} e on e.id = coalesce(prt.employee_id, prth.employee_id) and b.id = e.business_id
    join (select * from recent_payrun where rn = 1) rpr on rpr.employee_id = e.id and rpr.business_id = b.id 
    join {{ ref('keypay_tax_file_declaration') }} AS tfd ON tfd.id = e.tax_file_declaration_id and e.id = tfd.employee_id -- AU
    join {{ ref('keypay_employment_type') }} AS et ON et.id = tfd.employment_type_id
    join {{ ref('keypay_payrun_default') }} pd on e.pay_run_default_id = pd.id
  where
      (pr.date_first_finalised is not null and pr.date_first_finalised != 'NULL')      
      and (b.to_be_deleted is null or b.to_be_deleted = '' or b.to_be_deleted = 'NULL' or b.to_be_deleted = 'False') --ISNULL(b.to_be_deleted, 0) = 0
      and prt.total_hours < 24*31
      and not prt.is_excluded_from_billing
      and et.description = 'Full Time'      
      and pr.invoice_id is not null      
      and datediff('day', getdate(), b.commence_billing_from::date) < 180      
    group by 1,2,3,4,5,6
)

, annual_pay as (
    select employee_id, business_id, job_title, industry, residential_state, avg(monthly_wages) * 12 as annual_pay
    from (
        select "month", employee_id, job_title, business_id, industry, residential_state, monthly_wages, 
        ROW_NUMBER() OVER (PARTITION BY employee_id, business_id, job_title, industry, residential_state ORDER BY "month") AS month_num,
        ROW_NUMBER() OVER (PARTITION BY employee_id, business_id, job_title, industry, residential_state ORDER BY "month" desc) AS month_desc_num
        from  monthly_pay mp
    )
    --exclude the first and last month for the employee unless it's the past month
    where "month" >= DATEADD('month', -12, DATE_TRUNC('month', CURRENT_DATE))::date
    and month_num != 1
    and (month_desc_num != 1 or "month" = DATEADD('month', -1, DATE_TRUNC('month', CURRENT_DATE))::date)
    and job_title is not null and job_title!~ '^$' and len(job_title) > 1
    group by 1,2,3,4,5
    having annual_pay > 20000
    and annual_pay < 1000000    
)

, t_cleansed as (
    select job_title, {{job_title_cleaning('job_title')}} as t_title,
    business_id, employee_id, industry, residential_state, annual_pay
    from annual_pay
)

, t_common as (
  select t.job_title, INITCAP(coalesce(m.title_common, t.t_title)) as common_title,
    business_id, employee_id, industry, residential_state, annual_pay
  from t_cleansed t 
    left join csv.more_common_job_titles m on t.t_title = m.title_original
  ) 

select t.*,
ntile(3) over (partition by common_title, t.business_id order by annual_pay) as ntile_3_by_business,
ntile(3) over (partition by common_title, t.business_id, residential_state order by annual_pay) as ntile_3_by_business_state
from t_common t 