
    
    



select (industry || onboarded_month || country)
from "dev"."tableau"."monthly_onboardings_industry"
where (industry || onboarded_month || country) is null


