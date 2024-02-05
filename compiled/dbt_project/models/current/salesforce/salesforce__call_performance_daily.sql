with date_spine as (
    select
        date_trunc('day', date_day) as date_day
    from "dev"."salesforce"."int_salesforce__date_spine"
),

not_sat_demo_opp as (
    select opportunity_id
    from "dev"."salesforce"."stg_salesforce__opportunity"
    where demo_sat_date is NULL
),

task_call as (
    select
        date_trunc('day', task.activity_date) as activity_date,
        task.owner_id,
        sum(case
            when task.type in ('Call', 'Call - Reminder') then call_duration_in_seconds
            else 0
        end)                                  as total_call_duration_in_sec,
        sum(
            case
                when task.call_duration_in_seconds > 0 and task.type in ('Call', 'Call - Reminder') then 1
                else 0
            end
        )                                     as total_calls,
        sum(
            case
                when task.type in ('Connect', 'Connect - Meaningful', 'Connect - Non-Meaningful') then 1
                else 0
            end
        )                                     as total_connects,
        sum(
            case
                when task.type in ('Connect', 'Connect - Meaningful', 'Connect - Non-Meaningful') and opp.opportunity_id is not NULL then 1
                else 0
            end
        )                                     as total_connect_to_opps
    from "dev"."salesforce"."stg_salesforce__task" as task
    left join not_sat_demo_opp as opp
        on task.what_id = opp.opportunity_id
    group by 1, 2
),

final as (
    select
        date_spine.date_day,
        task_call.owner_id,
        task_call.total_call_duration_in_sec,
        task_call.total_calls,
        task_call.total_connects,
        task_call.total_connect_to_opps,
        round(
            case
                when task_call.total_connects then 100.0 * task_call.total_connect_to_opps / task_call.total_connects
                else 0
            end, 2
        )       as connect_to_opp_ratio,
        round(case
            when task_call.total_calls > 0 then 100.0 * task_call.total_connects / task_call.total_calls
            else 0
        end, 2) as call_to_connect_ratio
    from date_spine
    left join task_call
        on date_spine.date_day = task_call.activity_date
)

select * from final