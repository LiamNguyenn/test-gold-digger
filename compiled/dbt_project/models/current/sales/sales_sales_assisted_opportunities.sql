

select *
from "dev"."sales"."opportunities"
where
    record_type in ('Direct Sales', 'Upsell')
    and demo_sat_date is not null
    and (lost_reason != 'Demo did not sit' or lost_reason is null)