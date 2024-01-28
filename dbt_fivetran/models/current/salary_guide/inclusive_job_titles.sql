with sample_count as (
  select
  processed_title
  , count(distinct organisation_id) as orgs
  , count(*) as employees
from
  {{ref('eh_paying_employee_job_titles')}}
group by  1
having orgs > 4
and employees > 9
  )

select c.processed_title as base_title, c2.processed_title as prefix_title, null as suffix_title
from (select processed_title from sample_count c where processed_title ilike '% %') c -- more than 1 word
join (select processed_title from sample_count c where processed_title ilike '% %') c2 on c2.processed_title ilike '% ' || c.processed_title || '%'

union

select c.processed_title as base_title, null as prefix_title, c2.processed_title as suffix_title
from (select processed_title from sample_count c where processed_title ilike '% %') c  -- more than 1 word
join (select processed_title from sample_count c where processed_title ilike '% %') c2 on c2.processed_title ilike '%' || c.processed_title || ' %'

order by base_title
