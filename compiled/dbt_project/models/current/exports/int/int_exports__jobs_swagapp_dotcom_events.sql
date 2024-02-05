

with renamed as (
    select
        u.uuid                            as user_uuid,
        'candidate_account_created'::text as event_name,
        uinfo.updated_at                  as event_time,
        uinfo.created_at                  as date_created
    from "dev"."postgres_public"."user_infos" as uinfo
    left join "dev"."postgres_public"."users" as u
        on uinfo.user_id = u.id
    where
        1 = 1
        and source = 'career_page'
        and not coalesce(uinfo._fivetran_deleted, TRUE)
),

renamed_certifications as (
    select
        u2.uuid                               as user_uuid,
        'candidate_certification_added'::text as event_name,
        u1.created_at                         as event_time,
        u1.name                               as certification_name,
        date(u1.issued_date)                  as certification_issue_date,
        date(u1.expiry_date)                  as certification_end_date
    from "dev"."postgres_public"."user_certifications" as u1
    left join "dev"."postgres_public"."users" as u2
        on u1.user_id = u2.id
    


    -- this filter will only be applied in dev run
    
        where 1=1
    

        and not u1._fivetran_deleted
    qualify row_number() over (partition by u1.id order by u1.updated_at desc) = 1
),

union_all as (
    select
        user_uuid,
        event_name,
        event_time,
        date_created,
        NULL::text as certification_name,
        NULL::date as certification_issue_date,
        NULL::date as certification_end_date
    from renamed
    union all
    select
        user_uuid,
        event_name,
        event_time,
        NULL::date as date_created,
        certification_name,
        certification_issue_date,
        certification_end_date
    from renamed_certifications
)

select
    md5(cast(coalesce(cast(user_uuid as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(event_name as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(event_time as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as event_id,
    *
from union_all