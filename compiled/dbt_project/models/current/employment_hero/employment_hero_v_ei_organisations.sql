

with ei_bap as (
  select o.id as organisation_id  
  from "dev"."postgres_public"."business_accounts" as ba 
  join "dev"."postgres_public"."organisations" as o on ba.id = o.business_account_id  
  where not ba._fivetran_deleted
    and not o._fivetran_deleted
    and ba.name ilike '%employment innovations%'
)

, ei_whitelabel as (
  select distinct o.id as organisation_id  
  from "dev"."postgres_public"."organisations" as o
  join "dev"."employment_hero"."_v_connected_payrolls" epa on o.id = epa.organisation_id
  where business_account_id is null
  and json_extract_path_text(epa.data, 'kp_white_label') like '%Employment Innovations%'
  and not o._fivetran_deleted
)

select * from ei_bap
union
select * from ei_whitelabel