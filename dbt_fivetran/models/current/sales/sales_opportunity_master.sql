{{ config(alias='opportunity_master') }}

Select distinct
o.*,
rt.name record_type,
a.country,
p.name opp_owner_profile,
u.market_c opp_owner_market,
u.name opp_owner_name,
u.manager_name opp_manager_name,
uu.name lead_owner_name,
uu.manager_name lead_manager_name,
pp.name owner_profile,
uu.market_c owner_market,
--ARR computation
(case
 	when rt.name = 'Hero_Referrer' then 0
 	when rt.name = 'Organic' then o.amount
 	when oli.no_lineitem > 0 then oli.ARR
 	when o.quote_arr_c >0 then o.quote_arr_c
 	when o.stage_name = 'Won' then oli.ARR
 	when a.geo_code_c = 'AU' and rt.name = 'Direct Sales' then 12*13.36*opportunity_employees_c
 	when a.geo_code_c = 'AU' and rt.name = 'Upsell' then 12*3.56*opportunity_employees_c
 	when a.geo_code_c = 'NZ' and rt.name = 'Direct Sales' then 12*10.64*opportunity_employees_c
 	when a.geo_code_c = 'NZ' and rt.name = 'Upsell' then 12*2.65*opportunity_employees_c
 	when a.geo_code_c = 'SG' and rt.name = 'Direct Sales' then 12*5.00*opportunity_employees_c
 	when a.geo_code_c = 'SG' and rt.name = 'Upsell' then 12*1.25*opportunity_employees_c
  	when a.geo_code_c = 'MY' and rt.name = 'Direct Sales' then 12*9.99*opportunity_employees_c
 	when a.geo_code_c = 'MY' and rt.name = 'Upsell' then 12*2.51*opportunity_employees_c
 	when a.geo_code_c = 'UK' and rt.name = 'Direct Sales' then 12*6.65*opportunity_employees_c
 	when a.geo_code_c = 'UK' and rt.name = 'Upsell' then 12*1.66*opportunity_employees_c
 	else 0
end) as ARR
from
-- obtain a subset from opportunity
(select distinct
 id,
 owner_id,
 record_type_id,
 account_id,
 lead_source_type_c conversion_source_type,
 existing_customer_revenue_type_c,
 cast(created_date as date) created_date,
 cast(became_mql_date_c as date) became_mql_date,
 cast(Demo_Sat_Date_c as date) demo_sat_date,
 cast(close_date as date) close_date,
 lost_reason_c,
 opportunity_employees_c,
 stage_name,
 probability,
 amount,
 quote_arr_c,
 quote_srr_c,
 opportunity_originator_c,
 originating_lead_id_c,
 admin_opportunity_c,
 industry_c
from salesforce.opportunity
where is_deleted = 'False'
 --and admin_opportunity_c = 'False' --exclude admin opportunities
 --and cast(became_mql_date_c as date) >= dateadd(year,-2,current_date) -- set dates
) o
-- get record types
left join
(select distinct
id,
name
from
salesforce.record_type)
rt on rt.id = o.record_type_id
-- get opp owner info
left join
(select distinct
u1.id,
u1.name,
u1.profile_id,
u1.market_c,
u2.name manager_name
from
salesforce.user u1
-- get manager info
 left join
 salesforce.user u2 on u1.manager_id = u2.id
) u on u.id = o.owner_id
-- join profile
left join
(select distinct
id,
name
from salesforce.profile
where _fivetran_deleted = false
)p on p.id = u.profile_id


-- get originator/lead owner info
left join
(select distinct
u1.id,
u1.profile_id,
CASE WHEN u1.is_active = TRUE THEN u1.name ELSE 'Inactive' END AS name,
u1.market_c,
CASE WHEN u2.is_active = TRUE THEN u2.name ELSE 'Inactive' END AS manager_name
from
salesforce.user u1
-- get manager info
 left join
 salesforce.user u2 on u1.manager_id = u2.id
) uu on uu.id = o.opportunity_originator_c

left join
(select distinct
id,
name
from salesforce.profile
where _fivetran_deleted = false
)pp on pp.id = uu.profile_id

-- get account info
left join
(select distinct
id,
name,
account_number,
(case geo_code_c
when 'UK' then 'United Kingdom'
when 'AU' then 'Australia'
when 'SG' then 'Singapore'
when 'MY' then 'Malaysia'
when 'NZ' then 'New Zealand'
end) as country,
geo_code_c
from
salesforce.account
where is_deleted = 'False') a on a.id = o.account_id
-- opportunity lineitem for ARR
left join
(select distinct
 opportunity_id,
 count ( distinct id ) no_lineitem,
 sum((case when revenue_type_c = 'Monthly Recurring - Usage' then (list_price - discount_dollars_c) * quantity * 12
 else 0 end )) as ARR
 from salesforce.opportunity_line_item
 where is_deleted = 'False'
 group by opportunity_id) oli
 on oli.opportunity_id = o.id