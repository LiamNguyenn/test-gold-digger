
    
    

select
    (category || month || age_group) as unique_field,
    count(*) as n_records

from "dev"."employment_index"."pca_age_group"
where (category || month || age_group) is not null
group by (category || month || age_group)
having count(*) > 1


