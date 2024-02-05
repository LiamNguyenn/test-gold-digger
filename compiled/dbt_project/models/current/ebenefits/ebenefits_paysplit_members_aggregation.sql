


select 
    getdate()::date as date
    , count(member_id) as paysplit_members
    , count(case when has_wallet_account then 1 end) as paysplit_members_with_wallet
from 
    "dev"."ebenefits"."paysplit_members"

    where date >= (select max(date) from "dev"."ebenefits"."paysplit_members_aggregation")
