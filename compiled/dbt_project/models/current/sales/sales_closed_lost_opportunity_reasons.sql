

with
  opportunity_lost_reason as (
select distinct
  o.id as opportunity_id
  , o.lost_sub_reason_c as lost_reason
from
  "dev"."salesforce"."opportunity" o
where
  o.stage_name = 'Lost'
  and not o.is_deleted
  and o.lost_reason_c ilike 'product%'
  and o.lost_sub_reason_c is not null
  and o.lost_sub_reason_c not in ( 'Duplicate', 'Other', '' )
  
  and not exists (select 'x' from "dev"."sales"."closed_lost_opportunity_reasons" r where r.opportunity_id = o.id and r.lost_reason = o.lost_sub_reason_c)
   
union
select distinct
  o.id as opportunity_id
  , split_part(o.opp_Loss_Sub_Reason_c, ';', 1) as opp_lost_sub_reason
from
  "dev"."salesforce"."opportunity" o
where
  o.stage_name = 'Lost'
  and not o.is_deleted
  and o.lost_reason_c ilike 'product%'
  and opp_lost_sub_reason is not null
  and opp_lost_sub_reason not in ( 'Duplicate', 'Other', '' )
  
  and not exists (select 'x' from "dev"."sales"."closed_lost_opportunity_reasons" r where r.opportunity_id = o.id and r.lost_reason = opp_lost_sub_reason)
     
union
select distinct
  o.id as opportunity_id
  , split_part(o.opp_Loss_Sub_Reason_c, ';', 2) col2
from
  "dev"."salesforce"."opportunity" o
where
  o.stage_name = 'Lost'
  and not o.is_deleted
  and o.lost_reason_c ilike 'product%'
  and col2 is not null
  and col2 not in ( 'Duplicate', 'Other', '' )
    
  and not exists (select 'x' from "dev"."sales"."closed_lost_opportunity_reasons" r where r.opportunity_id = o.id and r.lost_reason = col2)
         
union
select distinct
  o.id as opportunity_id
  , split_part(o.opp_Loss_Sub_Reason_c, ';', 3) col3
from
  "dev"."salesforce"."opportunity" o
where
  o.stage_name = 'Lost'
  and not o.is_deleted
  and o.lost_reason_c ilike 'product%'
  and col3 is not null
  and col3 not in ( 'Duplicate', 'Other', '' )
  
  and not exists (select 'x' from "dev"."sales"."closed_lost_opportunity_reasons" r where r.opportunity_id = o.id and r.lost_reason = col3)
     
union
select distinct
  o.id as opportunity_id
  , split_part(o.opp_Loss_Sub_Reason_c, ';', 4) col4
from
  "dev"."salesforce"."opportunity" o
where
  o.stage_name = 'Lost'
  and not o.is_deleted
  and o.lost_reason_c ilike 'product%'
  and col4 is not null
  and col4 not in ( 'Duplicate', 'Other', '' )
  
  and not exists (select 'x' from "dev"."sales"."closed_lost_opportunity_reasons" r where r.opportunity_id = o.id and r.lost_reason = col4)
     
union
select distinct
  o.id as opportunity_id
  , split_part(o.opp_Loss_Sub_Reason_c, ';', 5) col5
from
  "dev"."salesforce"."opportunity" o
where
  o.stage_name = 'Lost'
  and not o.is_deleted
  and o.lost_reason_c ilike 'product%'
  and col5 is not null
  and col5 not in ( 'Duplicate', 'Other', '' )
  
  and not exists (select 'x' from "dev"."sales"."closed_lost_opportunity_reasons" r where r.opportunity_id = o.id and r.lost_reason = col5)
     
union
select distinct
  o.id as opportunity_id
  , split_part(o.opp_Loss_Sub_Reason_c, ';', 6) col6
from
  "dev"."salesforce"."opportunity" o
where
  o.stage_name = 'Lost'
  and not o.is_deleted
  and o.lost_reason_c ilike 'product%'
  and col6 is not null
  and col6 not in ( 'Duplicate', 'Other', '' )
  
  and not exists (select 'x' from "dev"."sales"."closed_lost_opportunity_reasons" r where r.opportunity_id = o.id and r.lost_reason = col6)
     
order by
  1, 2
    )

select
  r.*
  , f.workstream
  , f.product_line
  , f.product_family
from opportunity_lost_reason r
left join "dev"."eh_product"."opp_lost_ownership" as s on
  lower(r.lost_reason) = lower(s.lost_sub_reason)
left join "dev"."eh_product"."workstream_ownership" f on
  s.workstream = f.workstream