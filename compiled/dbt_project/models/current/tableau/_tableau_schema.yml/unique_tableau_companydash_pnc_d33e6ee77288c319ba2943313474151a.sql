
    
    

select
    (member_id || question || survey_name || score) as unique_field,
    count(*) as n_records

from "dev"."tableau"."tableau_companydash_pnc_surv"
where (member_id || question || survey_name || score) is not null
group by (member_id || question || survey_name || score)
having count(*) > 1


