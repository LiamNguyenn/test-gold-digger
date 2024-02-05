


select 
    getdate()::date as date
    , count(member_id) as instapay_members
    , count(case when first_time_swag_app is not null then 1 end) as instapay_members_using_swagapp
from 
    "dev"."ebenefits"."instapay_eligible_member_profile"

    where date >= (select max(date) from "dev"."ebenefits"."instapay_members_aggregation")
