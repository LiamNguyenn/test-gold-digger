with users as (
  select *
  from "dev"."zendesk"."stg_zendesk__user"

--If you use user tags this will be included, if not it will be ignored.

), user_tags as (

  select *
  from "dev"."zendesk"."stg_zendesk__user_tag"
  
), user_tag_aggregate as (
  select
    user_tags.user_id,
    
    listagg(user_tags.tags, ', ')

 as user_tags
  from user_tags
  group by 1



), final as (
  select 
    users.*

    --If you use user tags this will be included, if not it will be ignored.
    
    ,user_tag_aggregate.user_tags
    
  from users

  --If you use user tags this will be included, if not it will be ignored.
  
  left join user_tag_aggregate
    using(user_id)
  
)

select *
from final