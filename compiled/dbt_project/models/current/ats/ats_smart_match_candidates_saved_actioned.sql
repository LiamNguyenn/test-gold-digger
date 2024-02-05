

select
    "time",
    case when json_extract_path_text(properties, 'organisation_code') = '' then NULL else json_extract_path_text(properties, 'organisation_code') end                                                                                                                                                  as org_uuid,
    coalesce(case when json_extract_path_text(properties, 'user_code') = '' then NULL else json_extract_path_text(properties, 'user_code') end, case when json_extract_path_text(properties, 'candidate_user_code') = '' then NULL else json_extract_path_text(properties, 'candidate_user_code') end) as user_uuid,
    json_extract_path_text(properties, 'job_title')::varchar                                                                                                                                                                                                                                           as job_matched,
    json_extract_path_text(properties, 'raw_job_title')::varchar                                                                                                                                                                                                                                       as raw_job_matched,
    case when json_extract_path_text(properties, 'member_id') = '' then NULL else json_extract_path_text(properties, 'member_id') end                                                                                                                                                                  as employer_member_uuid,
    case when json_extract_path_text(properties, 'candidate_rank') = '' then NULL else json_extract_path_text(properties, 'candidate_rank') end                                                                                                                                                        as candidate_rank,
    case when json_extract_path_text(properties, 'candidate_session_id') = '' then NULL else json_extract_path_text(properties, 'candidate_session_id') end                                                                                                                                            as candidate_session_id,
    case when json_extract_path_text(properties, 'job_id') = '' then NULL else json_extract_path_text(properties, 'job_id') end                                                                                                                                                                        as job_id,
    case when json_extract_path_text(properties, 'location_source') = '' then NULL else json_extract_path_text(properties, 'location_source') end                                                                                                                                                      as location_source,
    mp.name                                                                                                                                                                                                                                                                                            as event_name
--  , case when app_version_string is not null then 'mobile' else 'web' end as app_type
from "dev"."mp"."event" as mp
where
    mp.name in ('Smart Match - Candidate saved', 'Smart Match - Candidate added in talent pool')
    -- 'Saved Candidates - Added to Talent Pool'
    and mp."time" >= '2023-07-25' --go live date  
    and mp."time" < (select date_trunc('day', max("time")) from "dev"."mp"."event")
