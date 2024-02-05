
    
    

select
    (category || month || company_size) as unique_field,
    count(*) as n_records

from "dev"."employment_index"."pca_company_size"
where (category || month || company_size) is not null
group by (category || month || company_size)
having count(*) > 1


