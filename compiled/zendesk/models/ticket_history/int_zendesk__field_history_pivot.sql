-- depends_on: "dev"."zendesk"."ticket_field_history"




    
with field_history as (

    select
        ticket_id,
        field_name,
        valid_ending_at,
        valid_starting_at

        --Only runs if the user passes updater fields through the final ticket field history model
        

        -- doing this to figure out what values are actually null and what needs to be backfilled in zendesk__ticket_field_history
        ,case when value is null then 'is_null' else value end as value

    from "dev"."zendesk"."int_zendesk__field_history_enriched"
    

), event_order as (

    select 
        *,
        row_number() over (
            partition by cast(valid_starting_at as date), ticket_id, field_name
            order by valid_starting_at desc
            ) as row_num
    from field_history

), filtered as (

    -- Find the last event that occurs on each day for each ticket

    select *
    from event_order
    where row_num = 1

), pivots as (

    -- For each column that is in both the ticket_field_history_columns variable and the field_history table,
    -- pivot out the value into it's own column. This will feed the daily slowly changing dimension model.

    select 
        ticket_id,
        cast(date_trunc('day', valid_starting_at) as date) as date_day

        
            
            ,min(case when lower(field_name) = 'assignee_id' then filtered.value end) as assignee_id

            --Only runs if the user passes updater fields through the final ticket field history model
            
        
            
            ,min(case when lower(field_name) = 'priority' then filtered.value end) as priority

            --Only runs if the user passes updater fields through the final ticket field history model
            
        
            
            ,min(case when lower(field_name) = 'status' then filtered.value end) as status

            --Only runs if the user passes updater fields through the final ticket field history model
            
        
    
    from filtered
    group by 1,2

), surrogate_key as (

    select 
        *,
        md5(cast(coalesce(cast(ticket_id as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(date_day as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as ticket_day_id
    from pivots

)

select *
from surrogate_key