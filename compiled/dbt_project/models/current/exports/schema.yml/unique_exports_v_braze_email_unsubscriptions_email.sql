
    
    

select
    email as unique_field,
    count(*) as n_records

from "dev"."exports"."exports_v_braze_email_unsubscriptions"
where email is not null
group by email
having count(*) > 1


