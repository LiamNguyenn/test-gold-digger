{{ config(materialized='table') }}

{%- set confidence_mapping = {
    'low': {'orgs': 13, 'samples': 16},
    'mid': {'orgs': 27, 'samples': 79},
    'high': {'orgs': 130, 'samples': 167},
    }
-%}

{%- set min_samples = {
    'orgs': 4,
    'samples': 9,
    }
-%}

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
        {{ ref('match_job_titles') }} 
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
    {%- for r in range(4) -%}
    {% for all_columns in modules.itertools.combinations(["residential_state", "industry", "seniority"], r) %}
    -- {{all_columns}}
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
        {% for key, value in confidence_mapping.items() -%}
        when m.orgs <= {{value['orgs']}} and m.samples <= {{value['samples']}} then '{{key}}'
        {% endfor -%}
        else 'very_high'
        end as confidence_level
    from 
        ( 
        select
            occupation,
            'AU' as country,
            {% if 'seniority' in all_columns %}'all' as {% endif -%}seniority,
            'all' as employment_type, -- placeholder, currently all data is for Full-time
            {% if 'residential_state' in all_columns %}'all' as {% endif -%}residential_state,
            {% if 'industry' in all_columns %}'all' as {% endif -%}industry, 
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
            {% if 'seniority' in all_columns %}'all' as {% endif -%}seniority, 
            'all' as employment_type, -- placeholder, currently all data is for Full-time
            {% if 'residential_state' in all_columns %}'all' as {% endif -%}residential_state, 
            {% if 'industry' in all_columns %}'all' as {% endif -%}industry,
            count(*) as samples, 
            count(distinct organisation_id) as orgs
        from resample
        group by 1,2,3,4,5,6
        having orgs > {{min_samples['orgs']}} and samples > {{min_samples['samples']}} 
        ) m on 
            t.occupation = m.occupation
            and t.country = m.country
            and t.seniority = m.seniority
            and t.employment_type = m.employment_type
            and t.residential_state = m.residential_state
            and t.industry = m.industry

    {% if not loop.last %}union all{% endif %}
    {%- endfor -%}
    {% if not loop.last %}union all{% endif %}
    {%- endfor -%}

    order by samples desc
    )
    , salary_ranges as (
        select 
            {{ dbt_utils.generate_surrogate_key(['occupation', 'country', 'seniority', 'employment_type', 'residential_state', 'industry']) }} as id
            , * 
        from by_state_industry_seniority
    )

select *, 'EH' as source from salary_ranges
-- union
-- -- -- add salary range from EH engineering bands for countries outside AU
-- select *, 'ENG' as source from {{ ref('eh_internal_engineering_salary_range') }} where id not in (select id from salary_ranges)
-- union
-- -- -- add salary range from EH non engineering roles (using AON data) where we do not have coverage
-- select *, 'AON' as source from {{ ref('eh_internal_non_engineering_salary_range') }} where id not in (select id from salary_ranges)