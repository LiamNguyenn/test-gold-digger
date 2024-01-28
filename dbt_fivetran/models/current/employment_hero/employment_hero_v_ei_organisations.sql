{{ config(materialized='view', alias='_v_ei_organisations') }}

with ei_bap as (
  select o.id as organisation_id  
  from {{ source('postgres_public', 'business_accounts') }} as ba 
  join {{ source('postgres_public', 'organisations') }} as o on ba.id = o.business_account_id  
  where not ba._fivetran_deleted
    and not o._fivetran_deleted
    and ba.name ilike '%employment innovations%'
)

, ei_whitelabel as (
  select distinct o.id as organisation_id  
  from {{ source('postgres_public', 'organisations') }} as o
  join {{ref('employment_hero_v_connected_payrolls')}} epa on o.id = epa.organisation_id
  where business_account_id is null
  and json_extract_path_text(epa.data, 'kp_white_label') like '%Employment Innovations%'
  and not o._fivetran_deleted
)

select * from ei_bap
union
select * from ei_whitelabel