with
    eh_eng_salary_data_to_add as (
        select
            occupation
            , country
            , seniority
            , employment_type
            , residential_state
            , industry
            , null::bigint as samples
            , null::bigint as orgs
            , round(0.85*p_25)::int as p10
            , p_25 as p25
            , p_50 as p50
            , p_75 as p75
            , round(1.05*p_75)::int as p90
            , 'high'::varchar(256) as confidence_level
        from 
            "dev"."salary_guide"."eh_engineering_salary_range"
        where
            country != 'AU'
            -- decision is to use the Salary Guide API data for AU so ignore the next condition
            -- or (country = 'AU' and seniority = 'manager' and occupation not in ('Software Developer - Machine Learning', 'Software Developer - Cloud'))
    )

select 
    md5(cast(coalesce(cast(occupation as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(country as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(seniority as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(employment_type as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(residential_state as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(industry as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as id
    , * 
from eh_eng_salary_data_to_add