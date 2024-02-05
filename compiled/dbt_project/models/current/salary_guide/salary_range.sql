with 
    resample as (
    select distinct 
        matched_title as occupation
        , lower(seniority) as seniority
        , organisation_id
        , residential_state
        , industry
        , ntile_3_by_org_state
        , median(annual_salary) over(partition by matched_title, seniority, organisation_id, residential_state, ntile_3_by_org_state) as sample_salary
    from 
        "dev"."salary_guide"."match_job_titles" 
    where 
        z_score_title_salary between -1.96 and 1.96
        and annual_salary > 10000
        and matched_title not in ('Test', 'Employee', 'Tbd', 'Not Applicable', 'Salary', 'St', 'Salaried')
        and matched_title not like 'Full Time%'
        and matched_title not like 'Full-Time%'
        and matched_title !~ '^(Senior|Level|Intermediate) [0-9]$'
        and matched_title !~ '^(Senior|Level|Intermediate)$'
    )

    , by_state_industry_seniority as (
    -- ()
    select 
        t.occupation
        , t.country
        , t.seniority
        , t.employment_type
        , t.residential_state
        , t.industry
        , m.samples
        , m.orgs
        , t.p10
        , t.p25
        , t.p50
        , t.p75
        , t.p90
        , case
        when m.orgs <= 13 and m.samples <= 16 then 'low'
        when m.orgs <= 27 and m.samples <= 79 then 'mid'
        when m.orgs <= 130 and m.samples <= 167 then 'high'
        else 'very_high'
        end as confidence_level
    from 
        ( 
        select
            occupation,
            'AU' as country,
            seniority,
            'all' as employment_type, -- placeholder, currently all data is for Full-time
            residential_state,
            industry, 
            (percentile_cont(0.1) within group (order by sample_salary))::int as p10, 
            (percentile_cont(0.25) within group (order by sample_salary))::int as p25, 
            (percentile_cont(0.5) within group (order by sample_salary))::int as p50, 
            (percentile_cont(0.75) within group (order by sample_salary))::int as p75,
            (percentile_cont(0.9) within group (order by sample_salary))::int as p90 
        from resample
        group by 1,2,3,4,5,6
        ) t
    join 
        (
        select 
            occupation, 
            'AU' as country,
            seniority, 
            'all' as employment_type, -- placeholder, currently all data is for Full-time
            residential_state, 
            industry,
            count(*) as samples, 
            count(distinct organisation_id) as orgs
        from resample
        group by 1,2,3,4,5,6
        having orgs > 4 and samples > 9 
        ) m on 
            t.occupation = m.occupation
            and t.country = m.country
            and t.seniority = m.seniority
            and t.employment_type = m.employment_type
            and t.residential_state = m.residential_state
            and t.industry = m.industry

    union all
    -- ('residential_state',)
    select 
        t.occupation
        , t.country
        , t.seniority
        , t.employment_type
        , t.residential_state
        , t.industry
        , m.samples
        , m.orgs
        , t.p10
        , t.p25
        , t.p50
        , t.p75
        , t.p90
        , case
        when m.orgs <= 13 and m.samples <= 16 then 'low'
        when m.orgs <= 27 and m.samples <= 79 then 'mid'
        when m.orgs <= 130 and m.samples <= 167 then 'high'
        else 'very_high'
        end as confidence_level
    from 
        ( 
        select
            occupation,
            'AU' as country,
            seniority,
            'all' as employment_type, -- placeholder, currently all data is for Full-time
            'all' as residential_state,
            industry, 
            (percentile_cont(0.1) within group (order by sample_salary))::int as p10, 
            (percentile_cont(0.25) within group (order by sample_salary))::int as p25, 
            (percentile_cont(0.5) within group (order by sample_salary))::int as p50, 
            (percentile_cont(0.75) within group (order by sample_salary))::int as p75,
            (percentile_cont(0.9) within group (order by sample_salary))::int as p90 
        from resample
        group by 1,2,3,4,5,6
        ) t
    join 
        (
        select 
            occupation, 
            'AU' as country,
            seniority, 
            'all' as employment_type, -- placeholder, currently all data is for Full-time
            'all' as residential_state, 
            industry,
            count(*) as samples, 
            count(distinct organisation_id) as orgs
        from resample
        group by 1,2,3,4,5,6
        having orgs > 4 and samples > 9 
        ) m on 
            t.occupation = m.occupation
            and t.country = m.country
            and t.seniority = m.seniority
            and t.employment_type = m.employment_type
            and t.residential_state = m.residential_state
            and t.industry = m.industry

    union all
    -- ('industry',)
    select 
        t.occupation
        , t.country
        , t.seniority
        , t.employment_type
        , t.residential_state
        , t.industry
        , m.samples
        , m.orgs
        , t.p10
        , t.p25
        , t.p50
        , t.p75
        , t.p90
        , case
        when m.orgs <= 13 and m.samples <= 16 then 'low'
        when m.orgs <= 27 and m.samples <= 79 then 'mid'
        when m.orgs <= 130 and m.samples <= 167 then 'high'
        else 'very_high'
        end as confidence_level
    from 
        ( 
        select
            occupation,
            'AU' as country,
            seniority,
            'all' as employment_type, -- placeholder, currently all data is for Full-time
            residential_state,
            'all' as industry, 
            (percentile_cont(0.1) within group (order by sample_salary))::int as p10, 
            (percentile_cont(0.25) within group (order by sample_salary))::int as p25, 
            (percentile_cont(0.5) within group (order by sample_salary))::int as p50, 
            (percentile_cont(0.75) within group (order by sample_salary))::int as p75,
            (percentile_cont(0.9) within group (order by sample_salary))::int as p90 
        from resample
        group by 1,2,3,4,5,6
        ) t
    join 
        (
        select 
            occupation, 
            'AU' as country,
            seniority, 
            'all' as employment_type, -- placeholder, currently all data is for Full-time
            residential_state, 
            'all' as industry,
            count(*) as samples, 
            count(distinct organisation_id) as orgs
        from resample
        group by 1,2,3,4,5,6
        having orgs > 4 and samples > 9 
        ) m on 
            t.occupation = m.occupation
            and t.country = m.country
            and t.seniority = m.seniority
            and t.employment_type = m.employment_type
            and t.residential_state = m.residential_state
            and t.industry = m.industry

    union all
    -- ('seniority',)
    select 
        t.occupation
        , t.country
        , t.seniority
        , t.employment_type
        , t.residential_state
        , t.industry
        , m.samples
        , m.orgs
        , t.p10
        , t.p25
        , t.p50
        , t.p75
        , t.p90
        , case
        when m.orgs <= 13 and m.samples <= 16 then 'low'
        when m.orgs <= 27 and m.samples <= 79 then 'mid'
        when m.orgs <= 130 and m.samples <= 167 then 'high'
        else 'very_high'
        end as confidence_level
    from 
        ( 
        select
            occupation,
            'AU' as country,
            'all' as seniority,
            'all' as employment_type, -- placeholder, currently all data is for Full-time
            residential_state,
            industry, 
            (percentile_cont(0.1) within group (order by sample_salary))::int as p10, 
            (percentile_cont(0.25) within group (order by sample_salary))::int as p25, 
            (percentile_cont(0.5) within group (order by sample_salary))::int as p50, 
            (percentile_cont(0.75) within group (order by sample_salary))::int as p75,
            (percentile_cont(0.9) within group (order by sample_salary))::int as p90 
        from resample
        group by 1,2,3,4,5,6
        ) t
    join 
        (
        select 
            occupation, 
            'AU' as country,
            'all' as seniority, 
            'all' as employment_type, -- placeholder, currently all data is for Full-time
            residential_state, 
            industry,
            count(*) as samples, 
            count(distinct organisation_id) as orgs
        from resample
        group by 1,2,3,4,5,6
        having orgs > 4 and samples > 9 
        ) m on 
            t.occupation = m.occupation
            and t.country = m.country
            and t.seniority = m.seniority
            and t.employment_type = m.employment_type
            and t.residential_state = m.residential_state
            and t.industry = m.industry

    union all
    -- ('residential_state', 'industry')
    select 
        t.occupation
        , t.country
        , t.seniority
        , t.employment_type
        , t.residential_state
        , t.industry
        , m.samples
        , m.orgs
        , t.p10
        , t.p25
        , t.p50
        , t.p75
        , t.p90
        , case
        when m.orgs <= 13 and m.samples <= 16 then 'low'
        when m.orgs <= 27 and m.samples <= 79 then 'mid'
        when m.orgs <= 130 and m.samples <= 167 then 'high'
        else 'very_high'
        end as confidence_level
    from 
        ( 
        select
            occupation,
            'AU' as country,
            seniority,
            'all' as employment_type, -- placeholder, currently all data is for Full-time
            'all' as residential_state,
            'all' as industry, 
            (percentile_cont(0.1) within group (order by sample_salary))::int as p10, 
            (percentile_cont(0.25) within group (order by sample_salary))::int as p25, 
            (percentile_cont(0.5) within group (order by sample_salary))::int as p50, 
            (percentile_cont(0.75) within group (order by sample_salary))::int as p75,
            (percentile_cont(0.9) within group (order by sample_salary))::int as p90 
        from resample
        group by 1,2,3,4,5,6
        ) t
    join 
        (
        select 
            occupation, 
            'AU' as country,
            seniority, 
            'all' as employment_type, -- placeholder, currently all data is for Full-time
            'all' as residential_state, 
            'all' as industry,
            count(*) as samples, 
            count(distinct organisation_id) as orgs
        from resample
        group by 1,2,3,4,5,6
        having orgs > 4 and samples > 9 
        ) m on 
            t.occupation = m.occupation
            and t.country = m.country
            and t.seniority = m.seniority
            and t.employment_type = m.employment_type
            and t.residential_state = m.residential_state
            and t.industry = m.industry

    union all
    -- ('residential_state', 'seniority')
    select 
        t.occupation
        , t.country
        , t.seniority
        , t.employment_type
        , t.residential_state
        , t.industry
        , m.samples
        , m.orgs
        , t.p10
        , t.p25
        , t.p50
        , t.p75
        , t.p90
        , case
        when m.orgs <= 13 and m.samples <= 16 then 'low'
        when m.orgs <= 27 and m.samples <= 79 then 'mid'
        when m.orgs <= 130 and m.samples <= 167 then 'high'
        else 'very_high'
        end as confidence_level
    from 
        ( 
        select
            occupation,
            'AU' as country,
            'all' as seniority,
            'all' as employment_type, -- placeholder, currently all data is for Full-time
            'all' as residential_state,
            industry, 
            (percentile_cont(0.1) within group (order by sample_salary))::int as p10, 
            (percentile_cont(0.25) within group (order by sample_salary))::int as p25, 
            (percentile_cont(0.5) within group (order by sample_salary))::int as p50, 
            (percentile_cont(0.75) within group (order by sample_salary))::int as p75,
            (percentile_cont(0.9) within group (order by sample_salary))::int as p90 
        from resample
        group by 1,2,3,4,5,6
        ) t
    join 
        (
        select 
            occupation, 
            'AU' as country,
            'all' as seniority, 
            'all' as employment_type, -- placeholder, currently all data is for Full-time
            'all' as residential_state, 
            industry,
            count(*) as samples, 
            count(distinct organisation_id) as orgs
        from resample
        group by 1,2,3,4,5,6
        having orgs > 4 and samples > 9 
        ) m on 
            t.occupation = m.occupation
            and t.country = m.country
            and t.seniority = m.seniority
            and t.employment_type = m.employment_type
            and t.residential_state = m.residential_state
            and t.industry = m.industry

    union all
    -- ('industry', 'seniority')
    select 
        t.occupation
        , t.country
        , t.seniority
        , t.employment_type
        , t.residential_state
        , t.industry
        , m.samples
        , m.orgs
        , t.p10
        , t.p25
        , t.p50
        , t.p75
        , t.p90
        , case
        when m.orgs <= 13 and m.samples <= 16 then 'low'
        when m.orgs <= 27 and m.samples <= 79 then 'mid'
        when m.orgs <= 130 and m.samples <= 167 then 'high'
        else 'very_high'
        end as confidence_level
    from 
        ( 
        select
            occupation,
            'AU' as country,
            'all' as seniority,
            'all' as employment_type, -- placeholder, currently all data is for Full-time
            residential_state,
            'all' as industry, 
            (percentile_cont(0.1) within group (order by sample_salary))::int as p10, 
            (percentile_cont(0.25) within group (order by sample_salary))::int as p25, 
            (percentile_cont(0.5) within group (order by sample_salary))::int as p50, 
            (percentile_cont(0.75) within group (order by sample_salary))::int as p75,
            (percentile_cont(0.9) within group (order by sample_salary))::int as p90 
        from resample
        group by 1,2,3,4,5,6
        ) t
    join 
        (
        select 
            occupation, 
            'AU' as country,
            'all' as seniority, 
            'all' as employment_type, -- placeholder, currently all data is for Full-time
            residential_state, 
            'all' as industry,
            count(*) as samples, 
            count(distinct organisation_id) as orgs
        from resample
        group by 1,2,3,4,5,6
        having orgs > 4 and samples > 9 
        ) m on 
            t.occupation = m.occupation
            and t.country = m.country
            and t.seniority = m.seniority
            and t.employment_type = m.employment_type
            and t.residential_state = m.residential_state
            and t.industry = m.industry

    union all
    -- ('residential_state', 'industry', 'seniority')
    select 
        t.occupation
        , t.country
        , t.seniority
        , t.employment_type
        , t.residential_state
        , t.industry
        , m.samples
        , m.orgs
        , t.p10
        , t.p25
        , t.p50
        , t.p75
        , t.p90
        , case
        when m.orgs <= 13 and m.samples <= 16 then 'low'
        when m.orgs <= 27 and m.samples <= 79 then 'mid'
        when m.orgs <= 130 and m.samples <= 167 then 'high'
        else 'very_high'
        end as confidence_level
    from 
        ( 
        select
            occupation,
            'AU' as country,
            'all' as seniority,
            'all' as employment_type, -- placeholder, currently all data is for Full-time
            'all' as residential_state,
            'all' as industry, 
            (percentile_cont(0.1) within group (order by sample_salary))::int as p10, 
            (percentile_cont(0.25) within group (order by sample_salary))::int as p25, 
            (percentile_cont(0.5) within group (order by sample_salary))::int as p50, 
            (percentile_cont(0.75) within group (order by sample_salary))::int as p75,
            (percentile_cont(0.9) within group (order by sample_salary))::int as p90 
        from resample
        group by 1,2,3,4,5,6
        ) t
    join 
        (
        select 
            occupation, 
            'AU' as country,
            'all' as seniority, 
            'all' as employment_type, -- placeholder, currently all data is for Full-time
            'all' as residential_state, 
            'all' as industry,
            count(*) as samples, 
            count(distinct organisation_id) as orgs
        from resample
        group by 1,2,3,4,5,6
        having orgs > 4 and samples > 9 
        ) m on 
            t.occupation = m.occupation
            and t.country = m.country
            and t.seniority = m.seniority
            and t.employment_type = m.employment_type
            and t.residential_state = m.residential_state
            and t.industry = m.industry

    order by samples desc
    )
    , salary_ranges as (
        select 
            md5(cast(coalesce(cast(occupation as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(country as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(seniority as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(employment_type as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(residential_state as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(industry as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as id
            , * 
        from by_state_industry_seniority
    )

select *, 'EH' as source from salary_ranges
-- union
-- -- -- add salary range from EH engineering bands for countries outside AU
-- select *, 'ENG' as source from "dev"."salary_guide"."eh_internal_engineering_salary_range" where id not in (select id from salary_ranges)
-- union
-- -- -- add salary range from EH non engineering roles (using AON data) where we do not have coverage
-- select *, 'AON' as source from "dev"."salary_guide"."eh_internal_non_engineering_salary_range" where id not in (select id from salary_ranges)