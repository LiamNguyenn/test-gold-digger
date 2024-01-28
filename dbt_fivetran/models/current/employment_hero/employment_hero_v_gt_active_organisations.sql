{{ config(materialized='view', alias='_v_gt_active_organisations') }}

select distinct host_organisation_id
from {{ref('employment_hero_v_gt_employees')}}
where active