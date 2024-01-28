{{ config(materialized="view", alias="_v_benefits_pillar_on_off_users") }}

with
    -- only checking user_infos, not member country  
    au_users as (
        select
            u.id as user_id,            
            case
                when
                    sum(
                        case
                            when
                                upper(ui.country_code) in ('AU', 'AUS')
                                and not ui._fivetran_deleted
                            then 1
                            else 0
                        end
                    )
                    > 0
                then true
                else false
            end as is_au            
        from {{ source('postgres_public', 'users') }} u
        join {{ source('postgres_public', 'user_infos') }} ui on u.id = ui.user_id
        where not u._fivetran_deleted 
            and not ui._fivetran_deleted
        group by 1
    )

, international_benefits as (    
    select u.id as user_id         
        , case when sum(case when o.international_benefits_enabled then 1 else 0 end) > 0 then true else false end as international_benefits_enabled        
    from {{ source('postgres_public', 'users') }} u
    join {{ source('postgres_public', 'members') }} m on m.user_id = u.id     
    join {{ref('ebenefits_v_international_benefits_on_off_organisations')}} o on m.organisation_id = o.organisation_id
    where not u._fivetran_deleted
        and not m._fivetran_deleted
        and m.active
    group by 1
)

select
    u.id as user_id,
    case when au.is_au and (bl.user_id is null) then true  -- country picker: AU, and not blacklisted    
        when bl.user_id is not null then false  -- blacklisted 
        when au.is_au is null then null  -- not picked country        
    end as au_benefits_enabled, 
    ib.international_benefits_enabled, 
    case when au_benefits_enabled or ib.international_benefits_enabled then true
        when not au_benefits_enabled or not ib.international_benefits_enabled then false        
        end as benefits_enabled  
from {{ source('postgres_public', 'users') }} u
left join {{ ref("ebenefits_v_benefits_pillar_blacklist_users") }} bl on bl.user_id = u.id
left join au_users au on u.id = au.user_id
left join international_benefits ib on ib.user_id = u.id 
    where not u._fivetran_deleted