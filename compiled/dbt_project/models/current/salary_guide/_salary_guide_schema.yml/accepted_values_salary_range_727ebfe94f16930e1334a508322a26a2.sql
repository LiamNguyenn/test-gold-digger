
    
    

with all_values as (

    select
        confidence_level as value_field,
        count(*) as n_records

    from "dev"."salary_guide"."salary_range"
    group by confidence_level

)

select *
from all_values
where value_field not in (
    'low','mid','high','very_high','insufficient_data'
)


