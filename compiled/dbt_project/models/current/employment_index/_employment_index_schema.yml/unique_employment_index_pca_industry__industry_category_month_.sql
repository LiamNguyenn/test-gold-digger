
    
    

select
    (industry || category || month) as unique_field,
    count(*) as n_records

from "dev"."employment_index"."pca_industry"
where (industry || category || month) is not null
group by (industry || category || month)
having count(*) > 1


