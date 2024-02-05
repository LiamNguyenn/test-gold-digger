
    
    

select
    (industry || onboarded_month || country) as unique_field,
    count(*) as n_records

from "dev"."tableau"."monthly_onboardings_industry"
where (industry || onboarded_month || country) is not null
group by (industry || onboarded_month || country)
having count(*) > 1


