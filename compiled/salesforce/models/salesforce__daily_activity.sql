with date_spine as (
    
    select 
        date_trunc('day', date_day) as date_day
    from "dev"."salesforce"."int_salesforce__date_spine"
),


task as (
    
    select 
        date_trunc('day', activity_date) as activity_date,
        count(task_id) as tasks_completed
    from "dev"."salesforce"."stg_salesforce__task"
    group by 1
), 



salesforce_event as (

    select 
        coalesce(date_trunc('day', activity_date), date_trunc('day', activity_date_time)) as activity_date,
        count(event_id) as events_completed
    from "dev"."salesforce"."stg_salesforce__event"  
    group by 1
), 



salesforce_lead as (

    select 
        date_trunc('day', created_date) as created_date,
        count(lead_id) as leads_created
    from "dev"."salesforce"."stg_salesforce__lead"
    group by 1
), 

salesforce_converted_lead as (

    select 
        date_trunc('day', converted_date) as converted_date,
        count(lead_id) as leads_converted
    from "dev"."salesforce"."stg_salesforce__lead"
    where is_converted
    group by 1
), 


opportunity as (

    select 
        opportunity_id,
        date_trunc('day', created_date) as created_date,
        account_id,
        date_trunc('day', close_date) as close_date,
        is_closed,
        is_deleted,
        is_won,
        owner_id, 
        stage_name,
        type,
        amount,
        case
            when is_won then 'Won'
            when not is_won and is_closed then 'Lost'
            when not is_closed and lower(forecast_category) in ('pipeline','forecast','bestcase') then 'Pipeline'
            else 'Other'
        end as status
    from "dev"."salesforce"."stg_salesforce__opportunity"
),

opportunities_created as (

    select
        created_date,
        count(opportunity_id) as opportunities_created,
        round(sum(amount)) as opportunities_created_amount
    from opportunity
    group by 1
),

opportunities_closed as (

    select
        close_date,
        count(case when status = 'Won' then opportunity_id else null end) as opportunities_won,
        round(sum(case when status = 'Won' then amount else 0 end)) as opportunities_won_amount,
        count(case when status = 'Lost' then opportunity_id else null end) as opportunities_lost,
        round(sum(case when status = 'Lost' then amount else null end)) as opportunities_lost_amount,
        round(sum(case when status = 'Pipeline' then amount else null end)) as pipeline_amount
    from opportunity
    group by 1
)

select
    date_spine.date_day,

    
    salesforce_lead.leads_created,
    salesforce_converted_lead.leads_converted,
    
    
    
    task.tasks_completed,
    

    
    salesforce_event.events_completed,
    

    opportunities_created.opportunities_created,
    opportunities_created.opportunities_created_amount,
    opportunities_closed.opportunities_won,
    opportunities_closed.opportunities_won_amount,
    opportunities_closed.opportunities_lost,
    opportunities_closed.opportunities_lost_amount,
    opportunities_closed.pipeline_amount
from date_spine


left join salesforce_lead
    on date_spine.date_day = salesforce_lead.created_date
left join salesforce_converted_lead
    on date_spine.date_day = salesforce_converted_lead.converted_date



left join task
    on date_spine.date_day = task.activity_date



left join salesforce_event
    on date_spine.date_day = salesforce_event.activity_date


left join opportunities_created
    on date_spine.date_day = opportunities_created.created_date
left join opportunities_closed
    on date_spine.date_day = opportunities_closed.close_date