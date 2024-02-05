
    
    

with all_values as (

    select
        seniority as value_field,
        count(*) as n_records

    from "dev"."salary_guide"."salary_range"
    group by seniority

)

select *
from all_values
where value_field not in (
    'all','junior','intermediate','senior','lead','manager','head','director','chief'
)


