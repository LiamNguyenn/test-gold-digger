with
user_info as (
    select
        u.user_id,
        addr.postcode,
        initcap(replace(u.first_name, chr(92) || chr(39), chr(39))) as first_name,
        initcap(replace(u.last_name, chr(92) || chr(39), chr(39)))  as last_name,
        u.address_id,
        u.country_code,
        u.phone_number,
        u.state_code,
        u.marketing_consented_at,
        coalesce(u.marketing_consented_at is not NULL, FALSE)       as is_marketing_consented,
        u.source                                                    as user_signin_source
    from "dev"."postgres_public"."user_infos" as u
    left join "dev"."postgres_public"."addresses" as addr
        on u.address_id = addr.id and not addr._fivetran_deleted
    where u.updated_at = (
        select max(updated_at) from "dev"."postgres_public"."user_infos"
        where user_id = u.user_id
    )
),

employee_latest as (
    select e1.*
    from "dev"."employment_hero"."employees" as e1
    where e1.updated_at = (
        select max(updated_at) from "dev"."employment_hero"."employees" as e2
        where e1.user_uuid = e2.user_uuid
    )
),

employee_address as (
    select
        e.user_uuid,
        e.address_id,
        addr.city,
        addr.country,
        addr.postcode,
        addr.state
    from employee_latest as e
    left join "dev"."postgres_public"."addresses" as addr
        on e.address_id = addr.id and not addr._fivetran_deleted
),

employment_histories as (
    select
        user_id,
        coalesce(
            case when industry_standard_job_title = '' then NULL else industry_standard_job_title end,
            case when job_title = '' then NULL else job_title end
        )           as job_title,
        current_job as still_in_position
    from "dev"."postgres_public"."user_employment_histories"
    where not _fivetran_deleted
    qualify row_number() over (partition by user_id order by updated_at desc) = 1
),

users as (
    select
        u.uuid                                              as user_uuid,
        u.id                                                as user_id,
        cc.alpha_two_letter,
        coalesce(
            case when 
    (case 
        when u.email is NULL then false
        when not (u.email not LIKE '%_@_%._%')
            and not (
		        u.email LIKE '.%'         -- begin with dot. eg: .abc@gmail.com
		        or u.email LIKE '%.@%'    -- email's name end with dot. eg: abc.@gmail.com
	        )

            -- Email domain should not contain numberic characters. eg: abc@gmai.123
	        and split_part(u.email, '.', regexp_count(u.email, '\\.') + 1) !~ '^[0-9]+$'

	        -- Length should not exceed 64 characters before @.
	        and length(split_part(u.email, '@', 1)) <= 64 then true
        else false
    end)
 = TRUE then u.email end,
            case when 
    (case 
        when e.personal_email is NULL then false
        when not (e.personal_email not LIKE '%_@_%._%')
            and not (
		        e.personal_email LIKE '.%'         -- begin with dot. eg: .abc@gmail.com
		        or e.personal_email LIKE '%.@%'    -- email's name end with dot. eg: abc.@gmail.com
	        )

            -- Email domain should not contain numberic characters. eg: abc@gmai.123
	        and split_part(e.personal_email, '.', regexp_count(e.personal_email, '\\.') + 1) !~ '^[0-9]+$'

	        -- Length should not exceed 64 characters before @.
	        and length(split_part(e.personal_email, '@', 1)) <= 64 then true
        else false
    end)
 = TRUE then e.personal_email end
        )                                                   as email,
        u.created_at                                        as user_date_created,

        case
            when e.gender is NULL or e.gender = '' or e.gender in ('Prefer not to say', 'Unknown', 'Prefer not to answer') then 'P'
            when e.gender in ('Non-binary', 'Other', 'Indeterminate') then 'O'
            when e.gender in ('Female', 'Females', 'Femalee', 'Femal', 'F') then 'F'
            when e.gender in ('Male', 'M') then 'M'
        end                                                 as gender,

        initcap(ea.city)                                    as home_city,

        coalesce(ui.postcode, ea.postcode)                  as postcode,
        coalesce(ui.address_id, ea.address_id)              as address_id,
        coalesce(ui.country_code, ea.country, cc.country)   as country,
        coalesce(ui.state_code, ea.state)                   as state_code,
        ui.first_name,
        ui.last_name,
        coalesce(ui.phone_number, e.personal_mobile_number) as phone_number,

        ui.marketing_consented_at,
        ui.is_marketing_consented,
        ui.user_signin_source,

        eh.job_title                                        as candidate_recent_job_title
    from "dev"."postgres_public"."users" as u
    left join user_info as ui
        on u.id = ui.user_id
    left join "dev"."workshop_public"."country_codes" as cc
        on ui.country_code = cc.alpha_two_letter or ui.country_code = cc.alpha_three_letter
    left join employee_latest as e
        on u.uuid = e.user_uuid
    left join employee_address as ea
        on u.uuid = ea.user_uuid
    left join employment_histories as eh
        on u.id = eh.user_id
    where not u._fivetran_deleted
),

user_braze_unsubscribed_lastest as (
    select
        user_id,
        email_address
    from (
        select
            *,
            row_number() over (
                partition by user_id, email_address
                order by time desc
            ) as rn
        from "dev"."braze"."subscription_event"
        where event_type = 'users.behaviors.subscription.GlobalStateChange'
    )
    where
        rn = 1
        and subscription_status = 'Unsubscribed'
),

user_marketo_unsubscribed_latest as (
    select
        email,
        unsubscribed
    from
        "dev"."marketo"."lead" as l
    where
        updated_at = (
            select max(updated_at)
            from "dev"."marketo"."lead"
            where
                email = l.email
        )
        and unsubscribed = TRUE
),

active_employed_users as (
    select distinct e.user_uuid
    from "dev"."employment_hero"."employees" as e
    where termination_date is NULL
    -- User gonna quite their job but already found a new job.
    or (
        select max(termination_date) from "dev"."employment_hero"."employees"
        where user_uuid = e.user_uuid
    ) < (
        select max(start_date) from "dev"."employment_hero"."employees"
        where user_uuid = e.user_uuid
    )
),

marketo_subscription as (
    select
        email,
        coalesce(whatsapp_subscribed is not NULL and whatsapp_subscribed = TRUE, FALSE) as marketo_whatsapp_opt_in,
        coalesce(email_subscribed is not NULL and email_subscribed = FALSE, FALSE)      as marketo_email_unsubscribed,
        coalesce(sms_subscribed is not NULL and sms_subscribed = FALSE, FALSE)          as marketo_sms_unsubscribed
    from "dev"."marketo"."subscription"
),

ebenefits as (
    select
        user_id,
        benefits_enabled
    from "dev"."ebenefits"."users"
)

select
    u.*,
    nvl2(e.user_uuid, TRUE, FALSE)                                             as user_actively_employed,
    case
        when c.user_uuid is not NULL then TRUE
        when u.user_signin_source = 'career_page' then TRUE
        else FALSE
    end                                                                        as user_is_candidate,
    ci.format_phone_number_e164(u.phone_number, u.alpha_two_letter) as phone_number_e164,
    marketo.marketo_whatsapp_opt_in,
    marketo.marketo_email_unsubscribed,
    marketo.marketo_sms_unsubscribed,

    coalesce(eb.benefits_enabled, FALSE)                                       as benefits_enabled
from users as u
left join user_braze_unsubscribed_lastest as ub
    on u.user_uuid = ub.user_id and u.email = ub.email_address
left join active_employed_users as e
    on u.user_uuid = e.user_uuid
left join "dev"."ats"."candidate_profiles" as c
    on u.user_uuid = c.user_uuid
left join marketo_subscription as marketo
    on u.email = marketo.email
left join ebenefits as eb
    on u.user_id = eb.user_id
where
    1 = 1
    -- filter out unsubscribed users from Marketo 
    and u.email not in (
        select email from user_marketo_unsubscribed_latest
    )
    -- filter out unsubscribed users from Braze
    and ub.user_id is NULL

    -- filter out empty name
    and u.first_name is not NULL and u.last_name != 'Unknown'
    and u.last_name is not NULL and u.last_name != 'Unknown'