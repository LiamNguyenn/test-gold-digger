
    
    



select (category || month || company_size)
from "dev"."employment_index"."pca_company_size"
where (category || month || company_size) is null


