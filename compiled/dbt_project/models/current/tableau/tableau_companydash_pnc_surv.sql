with
    survey_data as (
        select distinct
            s.name as survey_name,
            to_date(replace(substring(s.name, charindex('(', s.name) + 1, 8), ')', ''), 'dd/mm/yy') as date,
            ss.member_id,
            q.question_text,
            (
                case
                    when (lower(q.question_text) like '%which function%')
                    then cc.value
                    else regexp_replace(cc.value, '[^0-9]*', '')
                end
            ) as score,
            tc.value free_text_value
        from "dev"."survey_services_public"."custom_surveys" s
        left join
            "dev"."survey_services_public"."custom_survey_submissions" ss
            on ss.custom_survey_id = s.id
            and ss._fivetran_deleted = false
        left join
            "dev"."survey_services_public"."answers" a on ss.answer_id = a.id and a._fivetran_deleted = false
        left join
            "dev"."survey_services_public"."answer_details" ad
            on a.id = ad.answer_id
            and ad._fivetran_deleted = false
        left join
            "dev"."survey_services_public"."single_choice_answer_contents" cac
            on ad.content_id = cac.id
            and cac._fivetran_deleted = false
        left join
            "dev"."survey_services_public"."choice_contents" cc
            on cc.id = cac.choice_content_id
            and cc._fivetran_deleted = false
        left join
            "dev"."survey_services_public"."text_answer_contents" tc
            on tc.id = ad.content_id
            and tc._fivetran_deleted = false
        left join
            "dev"."survey_services_public"."questions" q
            on ad.question_id = q.id
            and q._fivetran_deleted = false
        where
            s.name like '%The EH Engagement & Enablement Survey%'
            and s.organisation_id = '3cfd1633-4920-488d-be7e-985df4acfd1b'
            and s._fivetran_deleted = false
            and s.all_employees = true
    )
select
    ('n178a9hid-fjk89ad7hf-' || em.member_uuid || '-jy987ahjadf') as member_id,  -- need to keep it this way due to some manual identifications needed on the organisational structure, will change this to dbt hash later
    em.if_active_employee,
    em.tenure,
    em.start_date,
    em.termination_date,
    em.gender,
    em.work_country,
    em.if_pass_probation,
    em.manager_name,
    em.path,
    s.survey_name,
    k.date survey_date,
    s.question_text as question,
    s.score,
    s.free_text_value
from "dev"."tableau"."tableau_companydash_eh_members_managers" em
inner join
    (select distinct date from survey_data) k
    on em.start_date <= k.date
    and (em.termination_date >= k.date or em.termination_date is null)
left join
    survey_data s
    on em.member_uuid = s.member_id
    and k.date = s.date
    and s.question_text not like '%Which function do you belong to%'
    and s.question_text not like '%Who is your direct manager%'