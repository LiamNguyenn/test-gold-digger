

with 
months as (
        select
          DATEADD('month', -generated_number::int, (date_trunc('month', add_months(current_date, 1))) )::date date
        from (

    

    with p as (
        select 0 as generated_number union all select 1
    ), unioned as (

    select

    
    p0.generated_number * power(2, 0)
     + 
    
    p1.generated_number * power(2, 1)
     + 
    
    p2.generated_number * power(2, 2)
     + 
    
    p3.generated_number * power(2, 3)
     + 
    
    p4.generated_number * power(2, 4)
     + 
    
    p5.generated_number * power(2, 5)
     + 
    
    p6.generated_number * power(2, 6)
     + 
    
    p7.generated_number * power(2, 7)
     + 
    
    p8.generated_number * power(2, 8)
    
    
    + 1
    as generated_number

    from

    
    p as p0
     cross join 
    
    p as p1
     cross join 
    
    p as p2
     cross join 
    
    p as p3
     cross join 
    
    p as p4
     cross join 
    
    p as p5
     cross join 
    
    p as p6
     cross join 
    
    p as p7
     cross join 
    
    p as p8
    
    

    )

    select *
    from unioned
    where generated_number <= 300
    order by generated_number

)
        where date >= '2012-01-01'
      ) 
      
, business_organisation_overlap as (
    select
      b.id
    from
      (
        select
          epa.organisation_id
          , external_id
        from
          "dev"."employment_hero"."_v_last_connected_payroll"
          as epa
          left join "dev"."postgres_public"."payroll_infos" on
            payroll_info_id = payroll_infos.id
            and not payroll_infos._fivetran_deleted
        where
          epa.type = 'KeypayAuth'
      )
      as o
      join "dev"."keypay_dwh"."business" b on
        b.id = o.external_id
  )
  , keypay_employees as (
    select distinct
      e.employee_id,
      e.business_id,
      case
        when first_paid_month < start_date then first_paid_month::date
        else start_date
      end as employee_start_date,
      end_date as termination_date
    from 
      "dev"."keypay"."au_pay_run_summary_s" e
      join 
        (select 
          employee_id
          , min(billing_month) as first_paid_month
        from
          "dev"."keypay"."au_pay_run_summary_s"
        group by 1) b 
        on e.employee_id = b.employee_id
    where
      e.business_id not in ( select * from business_organisation_overlap )
    )

  , business_employee_movement as (
    select 
      m.date, 
      e.business_id,
      count(distinct e.employee_id) as rolling_total
    from 
      months m 
      join keypay_employees e on m.date >= employee_start_date
        and (m.date <= e.termination_date or e.termination_date is null)
      group by 1,2
  )
  , business_growth_rate as (
    select
      'Keypay'::text as source
      , m.date
      , CONCAT('K', m.business_id) as id
      , m.business_id
      , cast(null as integer) as organisation_id
      , m.rolling_total
      , case
          when m.rolling_total < 20 then '1-19'
          when m.rolling_total > 19 and m.rolling_total < 200 then '20-199'
          when m.rolling_total > 199 then '200+'
        end as size
      , pre1.rolling_total as prev_1
      , pre3.rolling_total as prev_3
      , pre6.rolling_total as prev_6
      , pre12.rolling_total as prev_12      
      , (m.rolling_total-prev_1) / nullif(prev_1, 0 )::float as monthly_growth_rate
      , (m.rolling_total-prev_3) / nullif(prev_3, 0 )::float as quarterly_growth_rate
      , (m.rolling_total-prev_6) / nullif(prev_6, 0 )::float as semiannual_growth_rate
      , (m.rolling_total-prev_12) / nullif(prev_12, 0 )::float as annual_growth_rate
      , coalesce(industry, case when z.primary_industry is not null and z.primary_industry != '' then z.primary_industry else null end, null) as industry
      , coalesce(state, case when z.company_state = 'South Australia' then 'SA' when z.company_state = 'Northern Territory' then 'NT' when z.company_state = 'Victoria' then 'VIC' when z.company_state = 'New South Wales' then 'NSW' when z.company_state = 'Queensland' then 'QLD' when z.company_state = 'Tasmania' then 'TAS' when z.company_state = 'Western Australia' then 'WA' when z.company_state = 'Australian Capital Territory' then 'ACT' else null end, null) as state
    from
      business_employee_movement as m
      join(
        select
          b.id
          --         , case
          --             when industry_name ~* '^[0-9]+$' then null
          --             when industry_name ~* 'null' then null
          --             when len(industry_name)=1 then null
          --             else initcap(industry_name)
          --           end as industry
          , case
              when b.industry_id is null and b.industry_name is not null then 'Other'
              else i.name
            end as industry
          , case
              when state ~* '(South Australia|SA)' then 'SA'
              when state ~* '(Northern Territory|NT)' then 'NT'
              when state ~* '(Victoria|VIC)' then 'VIC'
              when state ~* '(New South|NSW)' then 'NSW'
              when state ~* '(Queensland|QLD)' then 'QLD'
              when state ~* '(Tasmania|TAS)' then 'TAS'
              when state ~* '(Western Australia|WA)' then 'WA'
              when state ~* '(Australian Capital Territory|ACT)' then 'ACT'
              else null
            end as state
          , region_id
        from
          "dev"."keypay_dwh"."business" b
        JOIN "dev"."keypay"."white_label" AS wl ON b.white_label_id = wl.id       
          left join "dev"."keypay_dwh"."suburb" s on
            b.suburb_id = s.id
          left join "dev"."keypay"."industry" i on
            b.industry_id = i.id
      ) b on
        m.business_id = b.id
      left join "dev"."keypay"."zoom_info" z on
        m.business_id = z._id
      left join business_employee_movement pre1 on m.business_id = pre1.business_id and m.date = dateadd('month', 1, pre1.date)
      left join business_employee_movement pre3 on m.business_id = pre3.business_id and m.date = dateadd('month', 3, pre3.date)
      left join business_employee_movement pre6 on m.business_id = pre6.business_id and m.date = dateadd('month', 6, pre6.date)
      left join business_employee_movement pre12 on m.business_id = pre12.business_id and m.date = dateadd('month', 12, pre12.date)  
    where
      (region_id is null or region_id = 1)
  )  
  , organisation_employee_movement as (
  select 
    m.date, 
    o.id as organisation_id,
    count(distinct e.id) as rolling_total
  from months m 
    join "dev"."employment_hero"."employees" e on m.date >= coalesce(e.start_date, e.created_at)
      and (m.date <= e.termination_date or e.termination_date is null)
   join "dev"."employment_hero"."organisations" o on e.organisation_id = o.id
  where o.pricing_type != 'demo' and o.country = 'AU'
    group by 1,2
  )

  , organisation_growth_rate as (
    select
      'EH'::text as source
      , m.date
      , CONCAT('E', m.organisation_id) as id
      , cast(null as integer) as business_id
      , m.organisation_id
      , m.rolling_total
      , case
          when m.rolling_total < 20 then '1-19'
          when m.rolling_total > 19 and m.rolling_total < 200 then '20-199'
          when m.rolling_total > 199 then '200+'
        end as size      
      , pre1.rolling_total as prev_1
      , pre3.rolling_total as prev_3
      , pre6.rolling_total as prev_6
      , pre12.rolling_total as prev_12      
      , (m.rolling_total-prev_1) / nullif(prev_1, 0 )::float as monthly_growth_rate
      , (m.rolling_total-prev_3) / nullif(prev_3, 0 )::float as quarterly_growth_rate
      , (m.rolling_total-prev_6) / nullif(prev_6, 0 )::float as semiannual_growth_rate
      , (m.rolling_total-prev_12) / nullif(prev_12, 0 )::float as annual_growth_rate    
      , o.industry
      , case
          when a.state is not null
          and a.state = '' then null
          else a.state
        end as state
    from
      organisation_employee_movement m
      left join "dev"."employment_hero"."organisations" as o on
        o.id = m.organisation_id
      left join "dev"."postgres_public"."addresses" a on
        o.primary_address_id = a.id
        and not a._fivetran_deleted
      left join organisation_employee_movement pre1 on m.organisation_id = pre1.organisation_id and m.date = dateadd('month', 1, pre1.date)
      left join organisation_employee_movement pre3 on m.organisation_id = pre3.organisation_id and m.date = dateadd('month', 3, pre3.date)
      left join organisation_employee_movement pre6 on m.organisation_id = pre6.organisation_id and m.date = dateadd('month', 6, pre6.date)
      left join organisation_employee_movement pre12 on m.organisation_id = pre12.organisation_id and m.date = dateadd('month', 12, pre12.date)  
  )

select 
  x.source
  , dateadd('month', -1, x.date) as date
  , x.id
  , x.business_id
  , x.organisation_id
  , x.rolling_total
  , x.size
  , x.monthly_growth_rate
  , x.quarterly_growth_rate
  , x.semiannual_growth_rate
  , x.annual_growth_rate  
  , (x.monthly_growth_rate-avg(x.monthly_growth_rate) over ()) / (stddev(x.monthly_growth_rate) over ()) as z_score_monthly_growth_rate
  , (x.quarterly_growth_rate-avg(x.quarterly_growth_rate) over ()) / (stddev(x.quarterly_growth_rate) over ()) as z_score_quarterly_growth_rate
  , (x.semiannual_growth_rate-avg(x.semiannual_growth_rate) over ()) / (stddev(x.semiannual_growth_rate) over ()) as z_score_semiannual_growth_rate
  , (x.annual_growth_rate-avg(x.annual_growth_rate) over ()) / (stddev(x.annual_growth_rate) over ()) as z_score_annual_growth_rate
  , case 
      when x.industry = 'Other' then 'Other'
      when x.industry != 'Other' and x.industry is not null then i.consolidated_industry 
      else null
    end as industry
  , x.state
from (
    select * from business_growth_rate
  union all
    select * from organisation_growth_rate
      ) as x 
  left join "dev"."one_platform"."industry" as i on
    regexp_replace( x.industry,'\\s','') = regexp_replace( i.eh_industry,'\\s','')
    or regexp_replace( x.industry,'\\s','') = regexp_replace( i.keypay_industry,'\\s','')
    or regexp_replace( x.industry,'\\s','') = regexp_replace( i.zoom_info_primary_industry,'\\s','')