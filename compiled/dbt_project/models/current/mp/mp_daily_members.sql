

select *
from
(
  select
	distinct date_trunc('day',e.timestamp) date
    , case
        when trim(user_id) ~ '^[0-9]+$' then trim(user_id)
        else null
    end::int as user_id
    , case
        when trim(member_id) ~ '^[0-9]+$' then trim(member_id)
        else null
    end::int as member_id
from "dev"."customers"."int_events" e
    where e.timestamp < (select date_trunc('day', max("timestamp")) from "dev"."customers"."int_events")
    

    and date_trunc('day', e.timestamp) > (select max(date) from "dev"."mp"."daily_members" )


)
where user_id is not null