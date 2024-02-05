

select distinct
    r.id,
    r.created_at                  as response_date,
    case r.properties_service
        when 'GuidedHR' then 'Guided HR'
        when 'GuidedPayroll' then 'Guided Payroll'
        when 'ManagedHR' then 'Managed HR'
        when 'ManagedPayroll' then 'Managed Payroll'
        else r.properties_service
    end                           as service,
    case r.properties_phase
        when 'KickOff' then 'Kick-off'
        else r.properties_phase
    end                           as phase,
    s.id                          as person_id,
    s.name,
    s.email,
    r.score,
    r.comment,
    r.permalink                   as survey_link,
    r.properties_delighted_source as source,
    r.properties_project_id       as asana_project_id,
    pj.account_c                  as account_id,
    pj.imp_proj_id
from
    "dev"."delighted_proserv_csat"."response" as r
inner join "dev"."delighted_proserv_csat"."person" as s
    on
        r.person_id = s.id
left join (
    select
        ap.asana_public_asana_project_id_c,
        imps.account_c,
        imps.service_offering_c,
        imps.id as imp_proj_id
    from
        "dev"."salesforce"."asana_public_asana_projects_relation_c" as ap
    inner join "dev"."salesforce"."implementation_project_c" as imps
        on
            ap.asana_public_object_id_c = imps.id
            and not imps.is_deleted and imps.status_c != 'Churned'
    where not ap.is_deleted
) as pj
    on r.properties_project_id = pj.asana_public_asana_project_id_c and ((r.properties_service ilike '%hr%' and pj.service_offering_c not ilike '%payroll%') or (r.properties_service ilike '%payroll%' and pj.service_offering_c not ilike '%hr%'))