
    
    

select
    task_id as unique_field,
    count(*) as n_records

from "dev"."salesforce"."stg_salesforce__task"
where task_id is not null
group by task_id
having count(*) > 1


