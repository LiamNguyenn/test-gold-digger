
    
    

with all_values as (

    select
        gender as value_field,
        count(*) as n_records

    from "dev"."exports"."exports_braze_users"
    group by gender

)

select *
from all_values
where value_field not in (
    'P','O','F','M'
)


