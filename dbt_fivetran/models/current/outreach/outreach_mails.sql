with
  max_mail_time as (
    select
      relationship_sequence_id
      , relationship_sequence_step_id
      , relationship_prospect_id
      , date(max(updated_at)) as last_action_date
    from
      outreach_raw.mailing
    where
      mailing_type = 'sequence'
      and updated_at is not null
    group by
      1
      , 2
      , 3
  )
select distinct
  m.id
  , (
    case
      when m.error_reason is null
        then null
      when m.error_reason not like '%5.%'
      and m.error_reason not like '%4.%' 
        then m.error_reason
      else 'untracked'
    end
  )
  as error_reason
  , m.state
  , case
    when u1.is_active = true
      then u1.name
    else 'Inactive'
  end as employee_name
  , case
    when u2.is_active = true
      then u2.name
    else 'Inactive'
  end as manager_name
  , ss.display_name step_name
  , ss.step_type
  , p.name prospect_name
  --pt.tag_name prospect_tag,
  , s.name sequence_name
  , st.tag_name sequence_tag
from
  outreach_raw.mailing m
  left join salesforce.user u1 on
    u1.email = m.mailbox_address
    and u1._fivetran_deleted = false
  -- get manager info
  left join salesforce.user u2 on
    u1.manager_id = u2.id
    and u2._fivetran_deleted = false
  inner join max_mail_time mmax on
    mmax.relationship_sequence_id = m.relationship_sequence_id
    and mmax.relationship_sequence_step_id = m.relationship_sequence_step_id
    and mmax.relationship_prospect_id = m.relationship_prospect_id
  inner join outreach_raw.sequence_step ss on
    ss.id = m.relationship_sequence_step_id
    and ss._fivetran_deleted = false
  inner join outreach_raw.sequence s on
    s.id = m.relationship_sequence_id
    and s._fivetran_deleted = false
  inner join outreach_raw.prospect p on
    p.id = m.relationship_prospect_id
    and p._fivetran_deleted = false
  --inner join outreach_raw.prospect_tag pt on pt.prospect_id = p.id 
  inner join outreach_raw.sequence_tag st on
    st.sequence_id = s.id
where
  m.mailing_type = 'sequence'
  and m.updated_at is not null
  and m._fivetran_deleted = false