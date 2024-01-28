{{ config(materialized='view', alias='_v_international_benefits_on_off_organisations') }}

with current_sub_plan as (
    select c.organisation_id     
    , s.name as sub_name
    from 
    (
        select *
        from {{ source('postgres_public', 'agreements') }}
        where id in (
            select
                FIRST_VALUE(id) over (partition by organisation_id order by created_at desc rows between unbounded preceding and unbounded following)
            from
                {{ source('postgres_public', 'agreements') }}
            where
                not _fivetran_deleted 
                and not cancelled
        )
    ) as c 
    left join {{ref('employment_hero_v_sub_plan_grouping')}} as s
        on c.subscription_plan_id = s.id
)

 select o.id as organisation_id
 , case when o.country != 'AU' and s.sub_name not ilike '%free%' and (b.organisation_uuid is null) then true 
   else false end as international_benefits_enabled
 from {{ source('postgres_public', 'organisations') }} o 
 left join current_sub_plan s on s.organisation_id = o.id 
 left join {{ref('ebenefits_v_international_benefits_refused_organisations')}} b on b.organisation_uuid = o.uuid
 where not o._fivetran_deleted