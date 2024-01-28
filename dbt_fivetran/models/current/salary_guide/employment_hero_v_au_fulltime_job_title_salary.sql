with salaries as (
    select distinct m.id as member_id, m.organisation_id, m.termination_date, h.title, o.industry,
    case
          when ea.state ~* '(South Australia|SA)' then 'SA'
          when ea.state ~* '(Northern Territory|NT)' then 'NT'
          when ea.state ~* '(Victoria|VIC)' then 'VIC'
          when ea.state ~* '(New South|NSW)' then 'NSW'
          when ea.state ~* '(Queensland|QLD)' then 'QLD'
          when ea.state ~* '(Tasmania|TAS)' then 'TAS'
          when ea.state ~* '(Western Australia|WA)' then 'WA'
          when ea.state ~* '(Australian Capital Territory|ACT)' then 'ACT'
          else null end as residential_state,      
    case when salary_type ~* 'hour' and hours_per_week > 0 then salary * hours_per_week * 52
         when salary_type ~* 'day' and days_per_week > 0 then salary * days_per_week * 52
         when salary_type ~* '^week' and days_per_week > 0 then salary * days_per_week * 52
         when salary_type ~* 'fortnight' and days_per_week > 0 then salary * days_per_week * 26
         when salary_type ~* '^month' then salary * 12 
         when salary_type ~* 'annum' then salary
         else 0 end as annual_salary
  from {{ ref('employment_hero_employees') }} m
  join {{ ref('employment_hero_organisations') }} o on m.organisation_id = o.id
  join 
      (select * from
        {{ source('postgres_public', 'employment_histories') }}
        where
            id in (
            select
                FIRST_VALUE(id) over(partition by member_id order by start_date desc rows between unbounded preceding and unbounded following)
            from
                {{ source('postgres_public', 'employment_histories') }}
            where
                not _fivetran_deleted
                and (end_date is null or datediff('month', end_date, CURRENT_DATE) < 12)
                and start_date < CURRENT_DATE
            )
    ) as h on m.id = h.member_id
  join
    (select * from
            {{ source('postgres_public', 'salary_versions') }}
            where
                id in (
                select
                    FIRST_VALUE(id) over(partition by member_id order by created_at desc rows between unbounded preceding and unbounded following)
                from
                    {{ source('postgres_public', 'salary_versions') }}
                where
                    not _fivetran_deleted
                    and effective_from < CURRENT_DATE            
                )
        ) as sv on sv.member_id = m.id
  left join {{ source('postgres_public', 'addresses') }} ea on m.address_id = ea.id and not ea._fivetran_deleted    
  where (active or datediff('month', m.termination_date, CURRENT_DATE) < 12 )  
    and annual_salary > 1000
    and annual_salary < 1000000  
    and employment_type='Full-time'   
    and h.title is not null and h.title !~ '^$' and len(h.title) !=1
    and o.pricing_tier != 'free'
    and o.country = 'AU'
    and (sv.currency = 'AUD' or sv.currency is null)
)

 , t_cleansed as (
    select title, {{job_title_cleaning('title')}} as t_title,
    organisation_id, industry, residential_state, member_id, termination_date, annual_salary
    from salaries
)

, t_common as (
    select t.title, trim(INITCAP(coalesce(m.title_common, t.t_title))) as common_title,
    organisation_id, industry, residential_state, member_id, termination_date, annual_salary    
    from t_cleansed t 
    left join {{source('csv', 'more_common_job_titles')}} m on t.t_title = m.title_original
)

select 
    title as job_title
    , common_title as processed_title
 	, organisation_id
    , member_id
    , termination_date
    , residential_state
    , annual_salary  
    , industry
from t_common