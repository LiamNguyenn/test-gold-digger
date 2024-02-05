
    
    

with all_values as (

    select
        employment_type as value_field,
        count(*) as n_records

    from "dev"."salary_guide"."salary_range"
    group by employment_type

)

select *
from all_values
where value_field not in (
    'all','casual','full-time','part-time'
)


