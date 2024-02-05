

select
  r.id
  , r.created_at as response_date
  , o.id as opportunity_id
  , o.account_id
  , p.name
  , p.email
  , r.score
  , r.permalink as survey_link
  , ra.free_response as comment
from
    "dev"."delighted_sales_csat"."response" as r
  left join "dev"."delighted_sales_csat"."person" as p on
    r.person_id = p.id
  left join "dev"."delighted_sales_csat"."response_answer" ra on
    r.id = ra.response_id
    and ra.question_id = 'text_HCtg6k'
  left join "dev"."salesforce"."opportunity" as o on
    r.properties_opportunity_id = o.id
    and not o.is_deleted


  -- this filter will only be applied on an incremental run    
    where not exists (select 'x' from "dev"."sales"."csat" c where c.id = r.id)

