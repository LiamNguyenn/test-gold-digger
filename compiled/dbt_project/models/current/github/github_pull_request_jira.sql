

select
  p.id as github_issue_id
  , p.title as github_issue_title
  , i.id as jira_issue_id
  , i.key as jira_issue_key
  , CONVERT_TIMEZONE('Australia/Sydney', getdate()) as _fivetran_transformed
from  "dev"."github"."issue" as p
    join "dev"."jira"."issue" as i on
    p.title ilike '%' + i.key + '%'
    and (CHARINDEX(lower(i.key), lower(p.title))=1 or substring(lower(p.title), CHARINDEX(lower(i.key), lower(p.title))-1,1) !~ '[[:alpha:]]')
    and (CHARINDEX(lower(i.key), lower(p.title))+len(i.key)=len(p.title)+1 or substring(lower(p.title), CHARINDEX(lower(i.key), lower(p.title))+len(i.key),1) !~ '[[:digit:]]')
where  not i._fivetran_deleted