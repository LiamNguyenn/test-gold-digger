





with validation_errors as (

    select
        date_day, dim_user_sk
    from "dev"."mart"."fct_mmau"
    group by date_day, dim_user_sk
    having count(*) > 1

)

select *
from validation_errors


