with 
annual_pay as (
    select member_id, organisation_id, industry, residential_state, avg(monthly_wages) * 12 as annual_pay
    from (
        select "month", member_id, organisation_id, industry, residential_state, monthly_wages, 
        ROW_NUMBER() OVER (PARTITION BY member_id ORDER BY "month") AS month_num,
        ROW_NUMBER() OVER (PARTITION BY member_id ORDER BY "month" desc) AS month_desc_num
        from  "dev"."employment_hero"."au_employee_monthly_pay" mp
        where "month" < DATE_TRUNC('month', CURRENT_DATE)::date -- this month not complete
        and work_country = 'AU'                                 -- only employees in AU
    )
    --exclude the first and last month for the employee unless it's the past month
    where "month" >= DATEADD('month', -12, DATE_TRUNC('month', CURRENT_DATE))::date
    and month_num != 1
    and (month_desc_num != 1 or "month" = DATEADD('month', -1, DATE_TRUNC('month', CURRENT_DATE))::date)
)

, sample_salaries as (
    select member_id, organisation_id, h.title, industry, residential_state, annual_pay as annual_salary
  from annual_pay p
  join 
      (select * from
        "dev"."postgres_public"."employment_histories"
      where
        id in (
          select
            FIRST_VALUE(id) over(partition by member_id order by created_at desc rows between unbounded preceding and unbounded following)
          from
            "dev"."postgres_public"."employment_histories"
          where
            not _fivetran_deleted
        )
    ) as h on m.id = h.member_id
  where --active
    --and o.pricing_tier != 'free'
    --and (sv.currency = 'AUD' or sv.currency is null)
    --and o.country = 'AU'
    employment_type='Full-time'
    and h.title is not null and h.title!~ '^$' and len(h.title) !=1
    and annual_pay > 20000
    and annual_pay < 1000000
)

, t_cleansed as (
    select title,  
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
                                    trim(REGEXP_REPLACE(title, '\\([^)]*\\)'))
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
    organisation_id, industry, residential_state, member_id, annual_salary
    from sample_salaries
)

, t_common as (
    select t.title, trim(INITCAP(coalesce(m.title_common, t.t_title))) as common_title,
    organisation_id, industry, residential_state, member_id, annual_salary
    from t_cleansed t 
    left join "dev"."csv"."more_common_job_titles" m on t.t_title = m.title_original
)
  
select *, 
case when stddev(annual_salary) over (partition by common_title) !=0 then (annual_salary-avg(annual_salary) over (partition by common_title)) / (stddev(annual_salary) over (partition by common_title)) else null end as z_score_title_salary,
ntile(3) over (partition by common_title, organisation_id order by annual_salary) as ntile_3_by_org,
ntile(3) over (partition by common_title, organisation_id, residential_state order by annual_salary) as ntile_3_by_org_state
from t_common