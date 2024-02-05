

select 
  *
from
  (
  select
    event.time
    -- reason we dont extract member_id, is cause it is attached to the demo org not the organic org
    -- organic org and its demo (playground) is not linked. the only way to get the link is through user_id
    , case when json_extract_path_text(properties, 'user_id') = '' then null else json_extract_path_text(properties, 'user_id')::integer end as user_id
    , name as event_name
    , case 
        when name = 'Users View Page' then trim('#' from regexp_substr(json_extract_path_text(properties, 'page'), '(.*)#'))
        else json_extract_path_text(properties, 'module') 
      end as page	
    , current_url
  from "dev"."mp"."event"
  where    
    json_extract_path_text(properties,'is_sandbox',true) ~ 'true'


    and event.time > (select max(time) from "dev"."mp"."playground_activities" )

  )