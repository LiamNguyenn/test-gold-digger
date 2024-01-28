{{
    config(       
        alias='employee_status_by_org'
    )
}}

with invites as (
    select e.id as member_id, min(i.created_at) as first_invited_at
    from
    {{source('workshop_public', 'invite_emails')}} as i 
    join {{ref('employment_hero_employees')}} as e
        on i.member_id = e.uuid         
    where not i._fivetran_deleted
    group by 1
)
    
, legit_emps as (
    select e.organisation_id, e.id as member_id, e.active,
    e.created_at,
    coalesce(least(i.first_invited_at, e.first_sign_in_at), case when e.accepted then e.created_at end) as invited_at,
    coalesce(case when e.accepted then coalesce(e.first_sign_in_at, invited_at) end) as activated_at, 
    row_number() over (partition by e.organisation_id order by e.created_at) as create_order,
    row_number() over (partition by e.organisation_id order by invited_at) as invite_order,
    row_number() over (partition by e.organisation_id order by activated_at) as activate_order
    from {{ref('employment_hero_employees')}} as e
    left join invites i on e.id = i.member_id
    where (e.termination_date is null or e.termination_date > e.created_at)
    -- counting deleted as well  
)

select * from legit_emps