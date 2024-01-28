{{ config(enabled=false, materialized="table", alias='au_fulltime_salary_feature') }}

-- any state or industry
  SELECT c.common_job_title, null as industry, null as residential_state, c.num_orgs, c.num_emps, c.salary_std, c.salary_cv, coalesce(s.entropy, 0) as state_diversity, coalesce(i.entropy, 0) as industry_diversity
  from(  SELECT common_job_title, count(distinct op_org_id) as num_orgs, count(*) as num_emps, STDDEV(annual_salary) as salary_std, salary_std/avg(annual_salary) as salary_cv
    from {{ ref('one_platform_v_au_fulltime_job_title_salary') }}  
    where z_score_title_salary between -1.96 and 1.96 
        and annual_salary > 20000
    group by 1
    )c        
  left join(
  SELECT common_job_title, -SUM(p * LOG(p)) / LOG(2)  AS entropy 
    FROM (
     SELECT common_job_title, residential_state, COUNT(*) AS count, COUNT(*)/(SUM(COUNT(*)) OVER (PARTITION BY common_job_title))::float AS p
     FROM {{ ref('one_platform_v_au_fulltime_job_title_salary') }}  
      where residential_state is not null
        and z_score_title_salary between -1.96 and 1.96 
        and annual_salary > 20000
     GROUP BY common_job_title, residential_state      
    )
  GROUP BY common_job_title
  )s on c.common_job_title = s.common_job_title
  left join (
  SELECT common_job_title, -SUM(p * LOG(p)) / LOG(2)  AS entropy 
    FROM (
     SELECT common_job_title, industry, COUNT(*) AS count, COUNT(*)/(SUM(COUNT(*)) OVER (PARTITION BY common_job_title))::float AS p
     FROM {{ ref('one_platform_v_au_fulltime_job_title_salary') }}    
      where industry is not null
        and z_score_title_salary between -1.96 and 1.96 
        and annual_salary > 20000
     GROUP BY common_job_title, industry      
    ) 
  GROUP BY common_job_title
  )i on c.common_job_title = i.common_job_title
where c.num_orgs > 5

union all
-- by state 
 SELECT c.common_job_title, null as industry, c.residential_state, c.num_orgs, c.num_emps, c.salary_std, c.salary_cv, 0 as state_diversity, coalesce(e.entropy, 0) as industry_diversity
  from(  SELECT common_job_title, residential_state, count(distinct op_org_id) as num_orgs, count(*) as num_emps, STDDEV(annual_salary) as salary_std, salary_std/avg(annual_salary) as salary_cv
    from {{ ref('one_platform_v_au_fulltime_job_title_salary') }}  
       where residential_state is not null
        and z_score_title_salary between -1.96 and 1.96 
        and annual_salary > 20000
    group by 1, 2
    )c        
  left join(
    SELECT common_job_title, residential_state, -SUM(p * LOG(p)) / LOG(2)  AS entropy 
        FROM (
         SELECT common_job_title, residential_state, industry, COUNT(*) AS count, COUNT(*)/(SUM(COUNT(*)) OVER (PARTITION BY common_job_title, residential_state))::float AS p
         FROM {{ ref('one_platform_v_au_fulltime_job_title_salary') }}  
          where industry is not null
            and z_score_title_salary between -1.96 and 1.96 
            and annual_salary > 20000
         GROUP BY common_job_title, residential_state, industry      
        ) 
      GROUP BY common_job_title, residential_state
  )e on c.common_job_title = e.common_job_title and c.residential_state = e.residential_state 
where c.num_orgs > 5

union all
-- by industry
  SELECT c.common_job_title, c.industry, null as residential_state, c.num_orgs, c.num_emps, c.salary_std, c.salary_cv, coalesce(e.entropy, 0) as state_diversity, 0 as industry_diversity
  from(  SELECT common_job_title, industry, count(distinct op_org_id) as num_orgs, count(*) as num_emps, STDDEV(annual_salary) as salary_std, salary_std/avg(annual_salary) as salary_cv
    from {{ ref('one_platform_v_au_fulltime_job_title_salary') }}  
       where industry is not null
        and z_score_title_salary between -1.96 and 1.96 
        and annual_salary > 20000
    group by 1, 2
    )c        
  left join(
      SELECT common_job_title, industry, -SUM(p * LOG(p)) / LOG(2)  AS entropy 
      FROM (
       SELECT common_job_title, residential_state, industry, COUNT(*) AS count, COUNT(*)/(SUM(COUNT(*)) OVER (PARTITION BY common_job_title, industry))::float AS p
       FROM {{ ref('one_platform_v_au_fulltime_job_title_salary') }}    
        where residential_state is not null
        and z_score_title_salary between -1.96 and 1.96 
        and annual_salary > 20000
       GROUP BY common_job_title, residential_state, industry      
      ) 
    GROUP BY common_job_title, industry
  )e on c.common_job_title = e.common_job_title and c.industry = e.industry
where c.num_orgs > 5

union all
-- by state and industry
    select common_job_title, industry, residential_state, count(distinct op_org_id) as num_orgs, count(*) as num_emps, STDDEV(annual_salary) as salary_std, salary_std/avg(annual_salary) as salary_cv, 0 as state_diversity, 0 as industry_diversity
  from {{ ref('one_platform_v_au_fulltime_job_title_salary') }}  
    where z_score_title_salary between -1.96 and 1.96 
        and annual_salary > 20000
  group by 1, 2, 3
having num_orgs > 5