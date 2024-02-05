
    
    

select
    (category || month) as unique_field,
    count(*) as n_records

from "dev"."employment_index"."pca_monthly_change"
where (category || month) is not null
group by (category || month)
having count(*) > 1


