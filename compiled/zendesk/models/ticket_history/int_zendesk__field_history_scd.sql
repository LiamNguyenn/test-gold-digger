-- model needs to materialize as a table to avoid erroneous null values
 



with change_data as (

    select *
    from "dev"."zendesk"."int_zendesk__field_history_pivot"

), set_values as (

-- each row of the pivoted table includes field values if that field was updated on that day
-- we need to backfill to persist values that have been previously updated and are still valid 
    select 
        date_day as valid_from,
        ticket_id,
        ticket_day_id

         

        ,priority
        ,sum(case when priority is null 
                then 0 
                else 1 
                    end) over (order by ticket_id, date_day rows unbounded preceding) as priority_field_partition
         

        ,status
        ,sum(case when status is null 
                then 0 
                else 1 
                    end) over (order by ticket_id, date_day rows unbounded preceding) as status_field_partition
         

        ,assignee_id
        ,sum(case when assignee_id is null 
                then 0 
                else 1 
                    end) over (order by ticket_id, date_day rows unbounded preceding) as assignee_id_field_partition
        

    from change_data

), fill_values as (
    select
        valid_from, 
        ticket_id,
        ticket_day_id

         

        ,first_value( priority ) over (partition by priority_field_partition, ticket_id order by valid_from asc rows between unbounded preceding and current row) as priority
        
         

        ,first_value( status ) over (partition by status_field_partition, ticket_id order by valid_from asc rows between unbounded preceding and current row) as status
        
         

        ,first_value( assignee_id ) over (partition by assignee_id_field_partition, ticket_id order by valid_from asc rows between unbounded preceding and current row) as assignee_id
        
        
    from set_values
) 

select *
from fill_values