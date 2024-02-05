with change_data as (

    select *
    from "dev"."zendesk"."int_zendesk__field_history_scd"
  
    

), calendar as (

    select *
    from "dev"."zendesk"."int_zendesk__field_calendar_spine"
    where date_day <= current_date
    

), joined as (

    select 
        calendar.date_day,
        calendar.ticket_id
        
             
            , priority
             
            , status
             
            , assignee_id
            
        

    from calendar
    left join change_data
        on calendar.ticket_id = change_data.ticket_id
        and calendar.date_day = change_data.valid_from
    
    

), set_values as (

    select
        date_day,
        ticket_id

        
        , priority
        -- create a batch/partition once a new value is provided
        , sum( case when priority is null then 0 else 1 end) over ( partition by ticket_id
            order by date_day rows unbounded preceding) as priority_field_partition

        
        , status
        -- create a batch/partition once a new value is provided
        , sum( case when status is null then 0 else 1 end) over ( partition by ticket_id
            order by date_day rows unbounded preceding) as status_field_partition

        
        , assignee_id
        -- create a batch/partition once a new value is provided
        , sum( case when assignee_id is null then 0 else 1 end) over ( partition by ticket_id
            order by date_day rows unbounded preceding) as assignee_id_field_partition

        

    from joined
),

fill_values as (

    select  
        date_day,
        ticket_id

        
        -- grab the value that started this batch/partition
        , first_value( priority ) over (
            partition by ticket_id, priority_field_partition 
            order by date_day asc rows between unbounded preceding and current row) as priority
        
        -- grab the value that started this batch/partition
        , first_value( status ) over (
            partition by ticket_id, status_field_partition 
            order by date_day asc rows between unbounded preceding and current row) as status
        
        -- grab the value that started this batch/partition
        , first_value( assignee_id ) over (
            partition by ticket_id, assignee_id_field_partition 
            order by date_day asc rows between unbounded preceding and current row) as assignee_id
        

    from set_values

), fix_null_values as (

    select  
        date_day,
        ticket_id
         

        -- we de-nulled the true null values earlier in order to differentiate them from nulls that just needed to be backfilled
        , case when  cast( priority as TEXT ) = 'is_null' then null else priority end as priority
         

        -- we de-nulled the true null values earlier in order to differentiate them from nulls that just needed to be backfilled
        , case when  cast( status as TEXT ) = 'is_null' then null else status end as status
         

        -- we de-nulled the true null values earlier in order to differentiate them from nulls that just needed to be backfilled
        , case when  cast( assignee_id as TEXT ) = 'is_null' then null else assignee_id end as assignee_id
        

    from fill_values

), surrogate_key as (

    select
        md5(cast(coalesce(cast(date_day as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(ticket_id as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as ticket_day_id,
        *

    from fix_null_values
)

select *
from surrogate_key