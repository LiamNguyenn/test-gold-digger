{{ config(alias='ticket') }}

with
  product_mapping as (
    select 
      po.product_family
      , wo.product_line
      , po.workstream
      , po.hr_page as product
      , po.product_owner
      , case when po.payroll_integration = 'no' then false else true end as payroll_integration
      , case 
          -- Billing was moved from General Settings sidebar
          when po.sidebar = 'Billings' then 'hr_endpoint_general_settings__billing'
    
          -- To cater for the legacy tagging on Recruitment sidebar 
          when po.sidebar = 'Recruitment' and lower(po.sub_category) = 'ats' then 'hr_endpoint_recruitment__ats_'
          when po.sidebar = 'Recruitment' and lower(po.sub_category) = 'job posting'
            then 'hr_endpoint_recruitment__ats___post_to_job_board'
          when po.sidebar = 'Recruitment' and lower(po.sub_category) = 'manage job board'
            then 'hr_endpoint_recruitment__ats___manage_job_board_'
          when po.sidebar = 'Recruitment' then 'hr_endpoint_recruitment__ats___' + lower(replace(po.sub_category, ' ', '_'))
    
          else 'hr_endpoint_' + lower(regexp_replace(replace(po.hr_page, '> ', '>'), '[^a-zA-Z0-9_&()>]+|[&()>]', '_')) 
        end as legacy_tag
    
      , 'employment_hero_hr_' + lower(regexp_replace(po.sidebar, '[^a-zA-Z0-9_&()]+|[&()]', '_')) as sidebar_tag
      , lower(regexp_replace((po.sidebar + '_' + po.sub_category), '[^a-zA-Z0-9_&()]+|[&()]', '_')) as sub_category_tag
      , lower(regexp_replace((po.sub_category + '_' + po.sub_sub_category), '[^a-zA-Z0-9_&()]+|[&()]', '_')) as sub_sub_category_tag
      , coalesce(sub_sub_category_tag, sub_category_tag, sidebar_tag) as feature_tag
    from
      {{ source('eh_product', 'product_ownership') }} as po
      left join {{ source('eh_product', 'workstream_ownership') }} as wo on
        po.workstream = wo.workstream
    order by
      po.hr_page
  )
  , ticket_region as (
    select
      ticket_id
      , ticket_region
      , payroll_integration
    from 
      (
        select distinct
          ticket.id as ticket_id, "tag"
          , case
              when ticket.custom_related_to_integration_true_if_checked_ and tt."tag" ~* '(payroll|keypay|xero|myob|qbo)' then 'ANZ'
              when ticket.custom_related_to_integration_true_if_checked_ and tt."tag" ~* '(singapore|uk)' then 'SEA_UK'
              else null
            end as ticket_region
          , case when ticket_region is null then 0 else 1 end as payroll_integration
          , row_number() over(partition by ticket.id order by ticket_region) as rn
        from
          {{ source('zendesk', 'ticket') }}
          join {{ source('zendesk', 'ticket_tag') }} as tt on
            ticket.id = tt.ticket_id
        where 
          ticket.status != 'deleted'
      )
    where rn = 1 
  )
  , ticket_tag as (
    select
      ticket_id
      , feature_tag
      , product_family
      , product_line
      , workstream
      , product
      , product_owner
      , ticket_region
    from
      (
        select distinct
          ticket_tag.ticket_id
          , case
              when po.sub_sub_category_tag is not null then 3
              when po.sub_category_tag is not null then 2
              when po.sidebar_tag is not null then 1
              else 0
            end as ticket_level
          , coalesce(po.sub_sub_category_tag, po.sub_category_tag, po.sidebar_tag, po.legacy_tag) as feature_tag
          , po.product_family
          , po.product_line
          , po.workstream
          , po.product
          , po.product_owner
          , tr.ticket_region
          , row_number() over(partition by ticket_tag.ticket_id order by ticket_level desc) as rn
        from 
          {{ source('zendesk', 'ticket_tag') }}
        left join ticket_region as tr on
          ticket_tag.ticket_id = tr.ticket_id
        left join product_mapping as po on
           ( ticket_tag."tag" = po.feature_tag
              or ticket_tag."tag" = po.legacy_tag )
            and tr.payroll_integration = po.payroll_integration
      )
    where rn = 1 
  )
select
  t.id
  , t.created_at
  , t.updated_at
  , t.subject
  , t.priority
  , t.status
  , t.via_channel
  , tfo_platform.name as platform
  , tfo_type.name as type
  , b.name as brand
  , g.name as assignee_group
  , r.name as requester
  , r.email as requester_email
  , tfo_requestor.name as requester_type
  , a.name as assignee
  , a.email as assignee_email
  , o.name as zendesk_organisation
  , tt.product
  , tt.workstream
  , tt.product_line
  , tt.product_family
  , tt.product_owner
  , tt.ticket_region as region
  , replace(nvl(theme_hr.name , theme_py.name), '::', ' > ') as theme
  , o.custom_organisation_id::integer as hr_org_id
from
  {{ source('zendesk', 'ticket') }} as t
  join {{ source('zendesk', 'brand') }} as b on
    t.brand_id = b.id
  left join zendesk.group as g on
    t.group_id = g.id
    and not g._fivetran_deleted
  left join {{ source('zendesk', 'user') }} as r on
    t.requester_id = r.id
  left join {{ source('zendesk', 'user') }} as a on
    t.assignee_id = a.id
  left join {{ source('zendesk', 'organization') }} as o on
    t.organization_id = o.id
  left join ticket_tag as tt on
    t.id = tt.ticket_id
  left join {{ source('zendesk', 'ticket_field_option') }} as tfo_platform on
    t.custom_product = tfo_platform."value"
  left join {{ source('zendesk', 'ticket_field_option') }} as tfo_type on
    coalesce(t.custom_enquiry_type_, t.custom_purpose_of_ticket) = tfo_type."value"
  left join {{ source('zendesk', 'ticket_field_option') }} as tfo_requestor on
    t.custom_end_user_type = tfo_requestor."value"
  left join {{ source('zendesk', 'ticket_field_option') }} as theme_hr on
    t.custom_specific_service_offering_hr_ = theme_hr."value"
  left join {{ source('zendesk', 'ticket_field_option') }} as theme_py on
    t.custom_specific_service_offering_payroll_ = theme_py."value"
where
  not b._fivetran_deleted
  and t.status != 'deleted'
order by
  t.created_at desc
  , t.id