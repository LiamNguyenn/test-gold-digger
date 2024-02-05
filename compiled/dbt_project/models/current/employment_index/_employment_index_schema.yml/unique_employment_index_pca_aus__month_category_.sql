
    
    

select
    (month || category) as unique_field,
    count(*) as n_records

from "dev"."employment_index"."pca_aus"
where (month || category) is not null
group by (month || category)
having count(*) > 1


