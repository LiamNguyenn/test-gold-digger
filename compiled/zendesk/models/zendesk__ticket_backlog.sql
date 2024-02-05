--This model will only run if 'status' is included within the `ticket_field_history_columns` variable.


with ticket_field_history as (
    select *
    from "dev"."zendesk"."zendesk__ticket_field_history"

), tickets as (
    select *
    from "dev"."zendesk"."stg_zendesk__ticket"

), group_names as (
    select *
    from "dev"."zendesk"."stg_zendesk__group"

), users as (
    select *
    from "dev"."zendesk"."stg_zendesk__user"

), brands as (
    select *
    from "dev"."zendesk"."stg_zendesk__brand"

--The below model is excluded if the user does not include ticket_form_id in the variable as a low percentage of accounts use ticket forms.


), organizations as (
    select *
    from "dev"."zendesk"."stg_zendesk__organization"

), backlog as (
    select
        ticket_field_history.date_day
        ,ticket_field_history.ticket_id
        ,ticket_field_history.status
        ,tickets.created_channel
         --Looking at all history fields the users passed through in their dbt_project.yml file
             --Standard ID field where the name can easily be joined from stg model.
                ,assignee.name as assignee_name

            
         --Looking at all history fields the users passed through in their dbt_project.yml file
             --All other fields are not ID's and can simply be included in the query.
                ,ticket_field_history.priority
            
        

    from ticket_field_history

    left join tickets
        on tickets.ticket_id = ticket_field_history.ticket_id

    

    

     --Join not needed if fields is not located in variable, otherwise it is included.
    left join users as assignee
        on assignee.user_id = cast(ticket_field_history.assignee_id as bigint)
    

    

    

    

    where ticket_field_history.status not in ('closed', 'solved', 'deleted')
)

select *
from backlog