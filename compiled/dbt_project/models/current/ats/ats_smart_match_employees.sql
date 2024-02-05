

select
    e.id                                                                               as member_id,
    e.user_id,
    u.uuid                                                                             as user_uuid,
    e.organisation_id,
    e.work_country,
    e.trim_job_title                                                                   as latest_job_title
    ,  
case
    when regexp_replace(trim_job_title, '^(Deputy |Casual )', '', 1, 'i') ~* 'assistant accountant' 
        then INITCAP(trim(regexp_replace(regexp_replace(trim_job_title, '^(Deputy |Casual )', '', 1, 'i'), '^(Graduate |Trainee |Associate |Apprentice |Junior |Intermediate |Senior |Lead |Principal |Chief |Head |Executive |Vice |Managing )', '', 1, 'i'))) 
    when regexp_replace(trim_job_title, '^(Deputy |Casual )', '', 1, 'i') !~* '^(Graduate |Associate |Assistant |Apprentice |Junior |Intermediate |Senior |Lead |Principal |Chief |Head |Executive |Vice |Managing )(of |to |\or |\and )'
        and regexp_replace(trim_job_title, '^(Deputy |Casual )', '', 1, 'i') !~* '^(Chief |Executive |Lead ).*(officer|assistant|generator).*'
    then INITCAP(trim(regexp_replace(regexp_replace(trim_job_title, '^(Deputy |Casual )', '', 1, 'i'), '^(Graduate |Trainee |Associate |Assistant |Apprentice |Junior |Intermediate |Senior |Lead |Principal |Chief |Head |Executive |Vice |Managing )', '', 1, 'i'))) 
    else INITCAP(regexp_replace(trim_job_title, '^(Deputy |Casual )', '', 1, 'i')) end
 as job_title_without_seniority
    ,  
case 
        when INITCAP(job_title_seniority) in ('Associate', 'Assistant', 'Graduate', 'Apprentice', 'Trainee') then 'Junior'
        when INITCAP(job_title_seniority) = '' or INITCAP(job_title_seniority) is null then 'Intermediate'
        when INITCAP(job_title_seniority) in ('Principal', 'Leader') then 'Lead'
        when INITCAP(job_title_seniority) in ('Managing') then 'Manager'
        when INITCAP(job_title_seniority) in ('Head') then 'Head'
        when INITCAP(job_title_seniority) in ('Vice', 'Executive') then 'Director'
        else INITCAP(job_title_seniority) end 
 as job_title_seniority,
    e.industry_standard_job_title,
    e.latest_employment_type,
    datediff('year', h.start_date, current_date)                                       as job_title_tenure,
    datediff('year', ha.first_start_date, current_date)                                as org_tenure,
    datediff('year', date_of_birth, current_date)                                      as age,
    addr.city,
    addr.postcode
from (select
    *,
    trim(latest_job_title) as trim_job_title,
    lower( 
case when regexp_replace(trim_job_title, '^(Deputy |Casual )', '', 1, 'i') ~ '^(Apprentice |Graduate |Trainee |Junior |Intermediate |Senior |Managing |Lead |Leader |Head |Vice |Manager |Director |Chief )' 
        then trim(regexp_substr(regexp_replace(trim_job_title, '^(Deputy |Casual )', '', 1, 'i'), '^(Apprentice |Graduate |Trainee |Junior |Intermediate |Senior |Managing |Lead |Leader |Head |Vice |Manager |Director |Chief )', 1, 1, 'i'))        
    when regexp_replace(trim_job_title, '^(Deputy |Casual )', '', 1, 'i') !~* '^(Associate |Assistant |Principal |Executive )(of |to )'
        and trim(regexp_substr(regexp_replace(trim_job_title, '^(Deputy |Casual )', '', 1, 'i'), '^(Associate |Assistant |Principal |Executive )', 1, 1, 'i')) != ''
        then trim(regexp_substr(regexp_replace(trim_job_title, '^(Deputy |Casual )', '', 1, 'i'), '^(Associate |Assistant |Principal |Executive )', 1, 1, 'i'))
    when trim_job_title ~ '(^|\\W)Apprentice(\\W|$)' then 'Apprentice'
    when trim_job_title ~ '(^|\\W)Graduate(\\W|$)' then 'Graduate'
    when trim_job_title ~ '(^|\\W)Junior(\\W|$)' then 'Junior'
    when trim_job_title ~ '(^|\\W)Intermediate(\\W|$)' then 'Intermediate'
    when trim_job_title ~ '(^|\\W)Senior(\\W|$)' then 'Senior'    
    when trim_job_title ~ '(^|\\W)Managing(\\W|$)' then 'Managing'
    when trim_job_title ~ '(^|\\W)(Lead|Leader)(\\W|$)' then 'Lead'
    when trim_job_title ~ '(^|\\W)Trainee(\\W|$)' then 'Trainee'
    when trim_job_title ~ '(^|\\W)Head(\\W|$)' then 'Head'
    when trim_job_title ~ '(^|\\W)Vice(\\W|$)' then 'Vice'
    when trim_job_title ~ '(^|\\W)Manager(\\W|$)' then 'Manager'
    when trim_job_title ~ '(^|\\W)Director(\\W|$)' then 'Director'
    when trim_job_title ~ '(^|\\W)Chief(\\W|$)' then 'Chief'
    else null end
)                          as job_title_seniority
from "dev"."employment_hero"."employees") as e
inner join "dev"."postgres_public"."users" as u on e.user_id = u.id
inner join "dev"."employment_hero"."organisations" as o on e.organisation_id = o.id
left join 

(
select
    *
  from
    "dev"."postgres_public"."employment_histories"
  where
    id in (
      select
        FIRST_VALUE(id) over(partition by member_id order by created_at desc rows between unbounded preceding and unbounded following)
      from
        "dev"."postgres_public"."employment_histories"
      where
        not _fivetran_deleted
    )
)

 as h
    on e.id = h.member_id
left join (select
    member_id,
    min(start_date) as first_start_date
from "dev"."postgres_public"."employment_histories" group by 1) as ha on e.id = ha.member_id
left join "dev"."postgres_public"."addresses" as addr on e.address_id = addr.id and not addr._fivetran_deleted
where
    e.active
    and o.is_paying_eh