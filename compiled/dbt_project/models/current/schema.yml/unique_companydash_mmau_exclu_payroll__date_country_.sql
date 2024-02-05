
    
    

select
    (date || country) as unique_field,
    count(*) as n_records

from "dev"."tableau"."companydash_mmau_exclu_payroll"
where (date || country) is not null
group by (date || country)
having count(*) > 1


