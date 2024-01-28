{{ config(materialized='view', alias='_v_instapay_on_off_organisations') }}

with payroll_auths as (
    select distinct organisation_id    
    , sum(case when json_extract_path_text(data, 'kp_white_label') in ('Employment Hero', 'Employment Innovations', 'Employment Innovations Internal', 'Employment Innovations internal UK', 'Employment Hero NZ', 'KeyPay', 'Lucent Advisory Pty Ltd') or json_extract_path_text(data, 'kp_white_label') = '' then 1 else 0 end) as eligible_white_label
    from 
    {{ref('employment_hero_v_connected_payrolls')}}
    group by 1
)

    select o.id as organisation_id 
    , case when epa.organisation_id is null then null   -- not linked to payroll, not eligible
           --when eo.id is null then null                 -- org deleted, not eligible 
           when o.pricing_tier not ilike '%free%' and o.payroll_type ilike '%Keypay%' and o.connected_app ilike '%Employment Hero Payroll%' 
                and epa.eligible_white_label > 0
                and bl.organisation_uuid is null then true           -- eligible and not blacklisted 
           when bl.organisation_uuid is not null then false          -- blacklisted 
           end as instapay_enabled 
from {{ref('employment_hero_organisations')}} as o
    left join payroll_auths epa on epa.organisation_id = o.id
    left join {{ref('ebenefits_v_instapay_blacklist_organisations')}} as bl on bl.organisation_uuid = o.uuid    