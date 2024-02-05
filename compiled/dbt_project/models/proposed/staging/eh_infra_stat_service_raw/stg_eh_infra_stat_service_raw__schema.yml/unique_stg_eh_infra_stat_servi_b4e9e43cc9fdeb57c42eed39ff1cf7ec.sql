
    
    

select
    id as unique_field,
    count(*) as n_records

from "dev"."staging"."stg_eh_infra_stat_service_raw__daily_report_sentry_issues"
where id is not null
group by id
having count(*) > 1


