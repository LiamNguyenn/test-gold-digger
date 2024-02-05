
    
    

select
    (residential_state || category || month) as unique_field,
    count(*) as n_records

from "dev"."employment_index"."pca_state"
where (residential_state || category || month) is not null
group by (residential_state || category || month)
having count(*) > 1


