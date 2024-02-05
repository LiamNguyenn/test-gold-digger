with ticket_field_history as (

    select *
    from "dev"."zendesk"."stg_zendesk__ticket_field_history"

), updater_info as (
    select *
    from "dev"."zendesk"."int_zendesk__updater_information"

), final as (
    select
        ticket_field_history.*

          

    from ticket_field_history

    left join updater_info
        on ticket_field_history.user_id = updater_info.updater_user_id
)
select *
from final