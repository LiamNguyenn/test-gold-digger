with organizations as (
    select * 
    from "dev"."zendesk"."stg_zendesk__organization"

--If you use organization tags this will be included, if not it will be ignored.

), organization_tags as (
    select * 
    from "dev"."zendesk"."stg_zendesk__organization_tag"

), tag_aggregates as (
    select
        organizations.organization_id,
        
    listagg(organization_tags.tags, ', ')

 as organization_tags
    from organizations

    left join organization_tags
        using (organization_id)

    group by 1


--If you use using_domain_names tags this will be included, if not it will be ignored.

), domain_names as (

    select *
    from "dev"."zendesk"."stg_zendesk__domain_name"

), domain_aggregates as (
    select
        organizations.organization_id,
        
    listagg(domain_names.domain_name, ', ')

 as domain_names
    from organizations

    left join domain_names
        using(organization_id)
    
    group by 1



), final as (
    select
        organizations.*

        --If you use organization tags this will be included, if not it will be ignored.
        
        ,tag_aggregates.organization_tags
        

        --If you use using_domain_names tags this will be included, if not it will be ignored.
        
        ,domain_aggregates.domain_names
        

    from organizations

    --If you use using_domain_names tags this will be included, if not it will be ignored.
    
    left join domain_aggregates
        using(organization_id)
    

    --If you use organization tags this will be included, if not it will be ignored.
    
    left join tag_aggregates
        using(organization_id)
    
)

select *
from final