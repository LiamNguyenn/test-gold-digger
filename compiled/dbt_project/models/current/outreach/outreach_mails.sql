with
max_mail_time as (
    select
        relationship_sequence_id,
        relationship_sequence_step_id,
        relationship_prospect_id,
        date(max(updated_at)) as last_action_date
    from
        "dev"."outreach_raw"."mailing"
    where
        mailing_type = 'sequence'
        and updated_at is not NULL
    group by
        1,
        2,
        3
)

select distinct
    m.id,
    (
        case
            when m.error_reason is NULL
                then NULL
            when
                m.error_reason not like '%5.%'
                and m.error_reason not like '%4.%'
                then m.error_reason
            else 'untracked'
        end
    )
    as error_reason,
    m.state,
    case
        when u1.is_active = TRUE
            then u1.name
        else 'Inactive'
    end             as employee_name,
    case
        when u2.is_active = TRUE
            then u2.name
        else 'Inactive'
    end             as manager_name,
    ss.display_name as step_name,
    ss.step_type,
    p.name          as prospect_name,
    --pt.tag_name prospect_tag,
    s.name          as sequence_name,
    st.tag_name     as sequence_tag
from
    "dev"."outreach_raw"."mailing" as m  -- noqa: AL06
left join "dev"."salesforce"."user" as u1  -- noqa: AL06
    on
        m.mailbox_address = u1.email
        and u1._fivetran_deleted = FALSE
-- get manager info
left join "dev"."salesforce"."user" as u2  -- noqa: AL06
    on
        u1.manager_id = u2.id
        and u2._fivetran_deleted = FALSE
inner join max_mail_time as mmax
    on
        m.relationship_sequence_id = mmax.relationship_sequence_id
        and m.relationship_sequence_step_id = mmax.relationship_sequence_step_id
        and m.relationship_prospect_id = mmax.relationship_prospect_id
inner join "dev"."outreach_raw"."sequence_step" as ss  -- noqa: AL06
    on
        m.relationship_sequence_step_id = ss.id
        and ss._fivetran_deleted = FALSE
inner join "dev"."outreach_raw"."sequence" as s  -- noqa: AL06
    on
        m.relationship_sequence_id = s.id
        and s._fivetran_deleted = FALSE
inner join "dev"."outreach_raw"."prospect" as p  -- noqa: AL06
    on
        m.relationship_prospect_id = p.id
        and p._fivetran_deleted = FALSE
inner join "dev"."outreach_raw"."sequence_tag" as st  -- noqa: AL06
    on
        s.id = st.sequence_id
where
    m.mailing_type = 'sequence'
    and m.updated_at is not NULL
    and m._fivetran_deleted = FALSE