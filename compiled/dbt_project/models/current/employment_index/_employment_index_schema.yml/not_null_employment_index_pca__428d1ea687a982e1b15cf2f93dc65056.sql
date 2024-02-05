
    
    



select (residential_state || category || month)
from "dev"."employment_index"."pca_state"
where (residential_state || category || month) is null


