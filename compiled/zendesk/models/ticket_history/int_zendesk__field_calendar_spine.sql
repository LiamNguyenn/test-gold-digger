

with calendar as (

    select *
    from "dev"."zendesk"."int_zendesk__calendar_spine"
    

), ticket as (

    select 
        *,
        -- closed tickets cannot be re-opened or updated, and solved tickets are automatically closed after a pre-defined number of days configured in your Zendesk settings
        cast( date_trunc('day', case when status != 'closed' then getdate() else updated_at end) as date) as open_until
    from "dev"."zendesk"."stg_zendesk__ticket"
    
), joined as (

    select 
        calendar.date_day,
        ticket.ticket_id
    from calendar
    inner join ticket
        on calendar.date_day >= cast(ticket.created_at as date)
        -- use this variable to extend the ticket's history past its close date (for reporting/data viz purposes :-)
        and 

    dateadd(
        month,
        0,
        ticket.open_until
        )

 >= calendar.date_day

), surrogate_key as (

    select
        *,
        md5(cast(coalesce(cast(date_day as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(ticket_id as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as ticket_day_id
    from joined

)

select *
from surrogate_key