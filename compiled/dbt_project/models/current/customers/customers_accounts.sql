

with
    eh_hr_accounts as (
        -- this view gets all accounts that were or are being billed for EH HR Software
        select distinct (sa.id) as external_id, sa.name
        from "dev"."zuora"."account" za
        inner join "dev"."salesforce"."account" sa on za.crm_id = sa.id
        inner join "dev"."zuora"."subscription" zs on zs.account_id = za.id
        left join "dev"."zuora"."rate_plan_charge" zrpc on zs.id = zrpc.subscription_id
        left join "dev"."zuora"."product_rate_plan" zprp on zrpc.product_rate_plan_id = zprp.id
        left join "dev"."zuora"."product" zp on zprp.product_id = zp.id
        where
            -- test accounts must be hard coded out of this view
            -- hardcode is the best way to do this so accounts with 'test' within the name is not accidentally
            -- removed, ie. contest
            za.batch != 'Batch50'
            and sa.id not in ('0010o00002p39mUAAQ', '0010o00002WeuIWAAZ')
            and not za._fivetran_deleted
            and not sa.is_deleted
            and not zs._fivetran_deleted
            and not zp._fivetran_deleted
            and not zprp._fivetran_deleted
            and not zrpc._fivetran_deleted
    ),
    paying_eh_orgs as (
        select o.*
        from "dev"."postgres_public"."organisations" o
        join
            (
                select *
                from "dev"."postgres_public"."agreements"
                where
                    id in (
                        select
                            first_value(id) over (
                                partition by organisation_id
                                order by created_at desc
                                rows between unbounded preceding and current row
                            )
                        from "dev"."postgres_public"."agreements"
                        where not cancelled
                    )
            ) a
            on o.id = a.organisation_id
        where
            not a._fivetran_deleted
            and not o._fivetran_deleted
            and not o.is_shadow_data
            and a.subscription_plan_id not in (  -- free
                4,  -- Startup Premium
                7,  -- Free (30 days)
                11,  -- Free
                17,  -- Demo
                43,  -- CHURN (FREE)
                52,  -- Implementations Free
                53,  -- Startup Standard
                55,  -- ANZ Free
                144,  -- International Free
                145,  -- Premium Trial
                161,  -- SUSPENDED (FREE) 
                162,  -- SEA free  
                166  -- ATS Free
            )
    ),
    stage_churn as (
        -- view with churn account details, ie.is_churn=0 means account churned
        select
            eh.external_id,
            eh.name,
            '[' || listagg(distinct '"' || za.id || '"', ', ') || ']' as zuora_id,
            '[' || listagg(distinct '"' || o.id || '"', ', ') || ']' as org_id,
            '[' || listagg(distinct '"' || ba.name || '"', ', ') || ']' as business_account_name,
            count(case when (zs.status = 'Active' or zs.status = 'Suspended') then 1 else null end) as is_churn
        from "dev"."zuora"."account" za
        inner join eh_hr_accounts eh on za.crm_id = eh.external_id
        inner join "dev"."zuora"."subscription" zs on zs.account_id = za.id
        left join "dev"."zuora"."rate_plan_charge" zrpc on zs.id = zrpc.subscription_id
        left join "dev"."zuora"."product_rate_plan" zprp on zrpc.product_rate_plan_id = zprp.id
        left join "dev"."zuora"."product" zp on zprp.product_id = zp.id
        left join
            "dev"."postgres_public"."organisations" o
            on za.id = o.zuora_account_id
            and not o._fivetran_deleted
            and not o.is_shadow_data
        left join
            "dev"."postgres_public"."business_accounts" ba
            on o.business_account_id = ba.id
            and not ba._fivetran_deleted
        where
            not za._fivetran_deleted
            and not zs._fivetran_deleted
            and not zp._fivetran_deleted
            and not zprp._fivetran_deleted
            and not zrpc._fivetran_deleted
            -- and zp.name ilike '%Software%'
            and zp.name ~* '(Software|Add-On Products)'
        group by eh.external_id, eh.name
    ),
    churn_date_field as (
        -- view with churn account date
        select
            scn.external_id,
            scn.name,
            -- , sa.churn_date_c, sa.downgrade_to_churn_date_c, zs.subscription_end_date, zs.cancelled_date,
            -- zs.term_end_date, sa.churn_request_date_c
            max(coalesce(zs.cancelled_date, zs.subscription_end_date, zs.term_end_date)) as churn_date
        from stage_churn as scn
        inner join "dev"."salesforce"."account" sa on sa.id = scn.external_id
        inner join "dev"."zuora"."account" za on za.crm_id = sa.id
        inner join "dev"."zuora"."subscription" zs on zs.account_id = za.id
        join "dev"."zuora"."rate_plan_charge" zrpc on zs.id = zrpc.subscription_id
        join "dev"."zuora"."product_rate_plan" zprp on zrpc.product_rate_plan_id = zprp.id
        join "dev"."zuora"."product" zp on zprp.product_id = zp.id
        inner join
            (
                -- get the last version of a subscription
                select s.account_id, s.name as sub_name, max(s.version) as version
                from "dev"."zuora"."subscription" s
                join "dev"."zuora"."account" a on s.account_id = a.id
                where not a._fivetran_deleted and not s._fivetran_deleted
                group by 1, 2
            ) cs
            on cs.sub_name = zs.name
            and zs.account_id = cs.account_id
            and cs.version = zs.version
        where
            scn.is_churn = 0
            and not sa.is_deleted
            and not za._fivetran_deleted
            and not zs._fivetran_deleted
            and not zrpc._fivetran_deleted
            and not zprp._fivetran_deleted
            and not zp._fivetran_deleted
            and zp.name ilike '%Software%'
        group by 1, 2
    ),
    stage_implementation as (
        -- churned accounts have already been filtered out.
        -- if account has 1 or more projects that are not completed, it is DEFINATELY under implementation
        -- (is_implementation is greater than 0)
        select
            scn.external_id,
            scn.name,
            '[' || listagg(distinct '"' || o.id || '"', ', ') || ']' as org_id,
            count(
                case
                    when
                        ipc_hr.id is not null
                        and (
                            coalesce(
                                ipc_hr.project_completion_date_c,
                                ipc_hr.go_live_date_c,
                                ipc_hr.actual_finish_date_c,
                                ipc_hr.completed_date_c
                            )
                            is null
                            or (
                                (
                                    ipc_hr.stage_c not in ('Go Live', 'Post Go Live', 'Cancelled', 'Expired', 'Closed')
                                    and ipc_hr.stage_c is not null
                                )
                                or (ipc_hr.status_c not in ('Completed', 'Closed') and ipc_hr.status_c is not null)
                            )
                        )
                        or ipc_pr.id is not null
                        and (
                            coalesce(
                                ipc_pr.project_completion_date_c,
                                ipc_pr.go_live_date_c,
                                ipc_pr.actual_finish_date_c,
                                ipc_pr.completed_date_c
                            )
                            is null
                            or (
                                -- ipc_pr.project_completion_date_c is null      or 
                                (
                                    ipc_pr.stage_c not in ('Go Live', 'Post Go Live', 'Cancelled', 'Expired', 'Closed')
                                    and ipc_pr.stage_c is not null
                                )
                                or (ipc_pr.status_c not in ('Completed', 'Closed') and ipc_pr.status_c is not null)
                            )
                        )
                    then 1  -- else null
                end
            ) as is_implementation

        from stage_churn as scn
        left join
            "dev"."salesforce"."implementation_project_c" ipc_hr
            on scn.external_id = ipc_hr.account_c
            and not ipc_hr.is_deleted
            and (ipc_hr.service_offering_c ilike '%hr%' or ipc_hr.service_offering_c ilike '%combined journey%')
        left join
            "dev"."salesforce"."implementation_project_c" ipc_pr
            on scn.external_id = ipc_pr.account_c
            and not ipc_pr.is_deleted
            and (ipc_pr.service_offering_c ilike '%payroll%' or ipc_hr.service_offering_c ilike '%combined journey%')
        left join
            "dev"."postgres_public"."organisations" o
            on scn.zuora_id = o.zuora_account_id
            and not o._fivetran_deleted
            and not o.is_shadow_data
        where scn.is_churn != 0
        group by scn.external_id, scn.name
    ),
    stage_completed as (
        -- accounts passed by the stage_implementation filter goes through a last filter to identify if:
        -- 1. there is an org_id (or multiple). if none then it will be in review. if there is then look at ALL the
        -- setup_mode for that account
        -- 2. in those orgs, how many setup_mode switched on is there. Greater than 0 means that it will be in the
        -- implementation stage
        -- there're some edge cases when customers leave setup_mode on permanently and use EH as a filing system
        -- without inviting their emps
        select
            si.external_id,
            si.name,
            '[' || listagg(
                distinct '"'
                || case when not o._fivetran_deleted and not o.is_shadow_data then o.id else null end
                || '"',
                ', '
            )
            || ']' as org_id,
            '['
            || listagg(distinct '"' || case when not kp.is_deleted then kp.id_c else null end || '"', ', ')
            || ']' as payroll_org_id,
            '[' || listagg(
                distinct '"' || case when o.setup_mode and not o._fivetran_deleted then o.id else null end || '"', ', '
            )
            || ']' as setup_mode_org_id
        from stage_implementation si
        inner join "dev"."zuora"."account" za on za.crm_id = si.external_id
        left join paying_eh_orgs o on za.id = o.zuora_account_id
        left join "dev"."salesforce"."keypay_org_c" kp on si.external_id = kp.linked_account_c
        where si.is_implementation = 0
        group by si.external_id, si.name
    ),
    account_stages as (
        -- this view puts the churn, implementation and completed view together to segregate the accounts
        -- an extra 'in review' stage is used for accounts that do not have a single org id attached
        select
            scn.external_id,
            scn.name,
            case
                when is_churn = 0 and getdate() >= cdf.churn_date
                then 'Churned'
                when sa.customer_stage_c = 'Offboarding' or (is_churn = 0 and getdate() < cdf.churn_date)
                then 'Offboarding'
                when is_implementation > 0
                then 'Implementation'
                -- when is_implementation = 0 and org_id is null then 'Live' -- maybe payroll-only
                when is_implementation = 0 and scm.org_id is null and scm.payroll_org_id is null
                then 'Implementation'  -- no HR and Payroll org
                when is_implementation = 0 and scm.org_id is null and scm.payroll_org_id is not null
                then 'Live'  -- maybe payroll-only  
                when is_implementation = 0 and scm.org_id is not null
                then 'Live'
            end as account_stage,
            cdf.churn_date,
            coalesce(scn.org_id, si.org_id, scm.org_id) as hr_org_id,
            business_account_name
        from stage_churn scn
        left join "dev"."salesforce"."account" sa on scn.external_id = sa.id and not sa.is_deleted
        left join stage_implementation si on scn.external_id = si.external_id
        left join stage_completed scm on scn.external_id = scm.external_id
        left join churn_date_field cdf on scn.external_id = cdf.external_id
    ),
    account_basic_details as (
        select
            sa.id as external_id,
            listagg(za.id, ', ') as billing_account_id,
            '[' || listagg(distinct '"' || za.account_number || '"', ', ') || ']' billing_account_number,
            '[' || listagg(distinct '"' || za.geo_code_c || '"', ', ') || ']' as zuora_geo,
            sa.name,
            sa.industry_primary_c as industry,  -- industry_primary_c field has data for more accounts compared to industry 
            -- za.currency,
            -- , sa.geo_code_c as sf_geo
            -- these 2 geo_code are the same
            convert_timezone('Australia/Sydney', sa.created_date) as created_date
        from "dev"."zuora"."account" za
        join "dev"."salesforce"."account" sa on za.crm_id = sa.id
        where not za._fivetran_deleted and not sa.is_deleted
        group by 1, 5, 6, 7
    ),
    subscription as (
        select
            eh.external_id,
            '['
            || listagg(distinct '"' || p.name || ' - ' || prp.name || '"', ', ') within group (order by p.name)
            || ']' subscription
        from eh_hr_accounts eh
        join "dev"."zuora"."account" a on a.crm_id = eh.external_id
        join "dev"."zuora"."subscription" s on s.account_id = a.id
        join "dev"."zuora"."rate_plan_charge" rpc on rpc.subscription_id = s.id
        join "dev"."zuora"."product_rate_plan" prp on rpc.product_rate_plan_id = prp.id
        join "dev"."zuora"."product" p on p.id = prp.product_id
        where
            not p._fivetran_deleted
            and not prp._fivetran_deleted
            and not rpc._fivetran_deleted
            and not a._fivetran_deleted
            and not s._fivetran_deleted
            and a.status = 'Active'
            and s.status != 'Expired'
            and s.status != 'Cancelled'
        group by 1
    ),
    account_contract_details as (
        select external_id, service_activation_date, term_end_date
        from
            (
                select
                    sa.id as external_id,
                    zp.name as product,
                    row_number() over (partition by sa.id order by sa.id, zp.name asc) as rn,
                    min(zs.service_activation_date) as service_activation_date,
                    max(zs.term_end_date) as term_end_date
                from "dev"."salesforce"."account" sa
                join "dev"."zuora"."account" za on za.crm_id = sa.id
                join "dev"."zuora"."subscription" zs on zs.account_id = za.id
                join "dev"."zuora"."rate_plan_charge" zrpc on zrpc.subscription_id = zs.id
                join "dev"."zuora"."product_rate_plan" zprp on zrpc.product_rate_plan_id = zprp.id
                join "dev"."zuora"."product" zp on zp.id = zprp.product_id
                where
                    not za._fivetran_deleted
                    and not zs._fivetran_deleted
                    and not zrpc._fivetran_deleted
                    and not zprp._fivetran_deleted
                    and not zp._fivetran_deleted
                    and zp.name in ('EH HR Software', 'EH Payroll Software')
                group by 1, 2
            )
        where rn = 1
    ),
    account_finance as (
        select distinct
            st.external_id,
            a.currency,
            sum(case when p.name = 'EH HR Software' then rpc.mrr end) as total_mrr_hr,
            sum(case when p.name = 'EH HR Software' then rpc.quantity end) as estimated_minimum_users_hr,
            round(total_mrr_hr / nullif(estimated_minimum_users_hr, 0), 2) list_price_per_unit_hr,
            sum(
                case
                    when p.name = 'EH HR Software' and rpc.mrr / nullif(rpc.quantity, 0) + 10 > prpct.price
                    then prpct.price - round(rpc.mrr / nullif(rpc.quantity, 0), 2)
                end
            ) as discount_price_per_unit_hr,
            sum(case when p.name = 'EH Payroll Software' then rpc.mrr end) as total_mrr_payroll,
            sum(case when p.name = 'EH Payroll Software' then rpc.quantity end) as estimated_minimum_users_payroll,
            round(total_mrr_payroll / nullif(estimated_minimum_users_payroll, 0), 2) list_price_per_unit_payroll,
            sum(
                case
                    when p.name = 'EH Payroll Software' and rpc.mrr / nullif(rpc.quantity, 0) + 10 > prpct.price
                    then prpct.price - round(rpc.mrr / nullif(rpc.quantity, 0), 2)
                end
            ) as discount_price_per_unit_payroll,
            sum(
                case when p.name != 'EH HR Software' and p.name != 'EH Payroll Software' then rpc.mrr end
            ) as total_mrr_addon,
            sum(
                case when p.name != 'EH HR Software' and p.name != 'EH Payroll Software' then rpc.quantity end
            ) as estimated_minimum_users_addon,
            round(total_mrr_addon / nullif(estimated_minimum_users_addon, 0), 2) list_price_per_unit_addon,
            sum(
                case
                    when
                        p.name != 'EH HR Software'
                        and p.name != 'EH Payroll Software'
                        and rpc.mrr / nullif(rpc.quantity, 0) + 10 > prpct.price
                    then prpct.price - round(rpc.mrr / nullif(rpc.quantity, 0), 2)
                end
            ) as discount_price_per_unit_addon
        from eh_hr_accounts st
        join "dev"."zuora"."account" a on st.external_id = a.crm_id
        join "dev"."zuora"."rate_plan_charge" rpc on a.id = rpc.account_id
        join
            (
                select a.id as account_id, s.name as subscription, max(s.version) as version
                from "dev"."zuora"."subscription" s
                join "dev"."zuora"."account" a on s.account_id = a.id
                where
                    not a._fivetran_deleted
                    and a.status = 'Active'
                    and s.status != 'Expired'
                    and s.status != 'Cancelled'
                    and (s.subscription_end_date >= getdate() or s.subscription_end_date is null)
                    and not s._fivetran_deleted
                group by a.id, s.name
            ) za
            -- rpc.subscription_id = za.subscription_id
            on rpc.version = za.version
            and rpc.account_id = za.account_id
        join "dev"."zuora"."product_rate_plan" prp on rpc.product_rate_plan_id = prp.id
        join "dev"."zuora"."product_rate_plan_charge" prpc on prp.id = prpc.product_rate_plan_id
        join "dev"."zuora"."product" p on prp.product_id = p.id
        join
            "dev"."zuora"."product_rate_plan_charge_tier" prpct
            on prpc.id = prpct.product_rate_plan_charge_id
            and not prpct._fivetran_deleted
            and prpct.active
            and a.currency = prpct.currency
            and rpc.quantity between round(prpct.starting_unit) and coalesce(round(prpct.ending_unit) - 1, 999)
            and prp.product_id = prpct.product_id
        where
            rpc.effective_start_date <= getdate()
            and (rpc.effective_end_date > getdate() or rpc.effective_end_date is null)
            and not rpc._fivetran_deleted
            and rpc.name like 'Contracted%'
            and not prp._fivetran_deleted
            and not prpc._fivetran_deleted
            and not p._fivetran_deleted
        group by 1, 2
    ),
    account_cmrr as (
        select
            sa.id as external_id, sum(za.balance::decimal(9, 2)) outstanding_balance, sum(za.mrr::decimal(9, 2)) as cmrr
        from "dev"."zuora"."account" za
        join "dev"."salesforce"."account" sa on za.crm_id = sa.id
        where not za._fivetran_deleted and not sa.is_deleted
        group by 1
    ),
    account_mrr as (
        select external_id, invoice_month, invoice_amount as mrr
        from
            (
                select
                    account.crm_id as external_id,
                    dateadd(month, 1, date_trunc('month', invoice.invoice_date) - '1 day'::interval) as invoice_month,
                    sum(invoice_item.charge_amount)::decimal(9, 2) as invoice_amount,
                    row_number() over (partition by account.crm_id order by invoice_month desc) as rn
                from "dev"."zuora"."account"
                join "dev"."zuora"."invoice" on account.id = invoice.account_id
                join "dev"."zuora"."invoice_item" on invoice.id = invoice_item.invoice_id
                join "dev"."zuora"."product" zp on invoice_item.product_id = zp.id
                where
                    not account._fivetran_deleted
                    and not invoice._fivetran_deleted
                    and not invoice_item._fivetran_deleted
                    and invoice.status = 'Posted'
                    and zp.name != 'Services'
                    and not zp._fivetran_deleted
                group by account.crm_id, invoice_month
            )
        where rn = 1
    ),
    account_employees as (
        select
            sa.id as external_id,
            count(
                distinct(case when m.active and m.accepted and not m.independent_contractor then u.uuid else null end)
            ) as active_employees,
            count(
                distinct(
                    case when m.active and not m.accepted and not m.independent_contractor then u.uuid else null end
                )
            ) as pending_employees,
            (active_employees + pending_employees) as active_and_pending_employees,
            count(
                distinct(case when m.active and m.independent_contractor then u.uuid else null end)
            ) as independent_contractors,
            count(
                distinct(case when not m.active and m.termination_date is not null then u.uuid else null end)
            ) as terminated_employees
        from "dev"."salesforce"."account" sa
        join "dev"."zuora"."account" za on za.crm_id = sa.id
        left join paying_eh_orgs o on za.id = o.zuora_account_id
        left join "dev"."postgres_public"."members" m on o.id = m.organisation_id
        inner join "dev"."postgres_public"."users" u on u.id = m.user_id
        where
            not sa.is_deleted
            and not za._fivetran_deleted
            and not o._fivetran_deleted
            and not o.is_shadow_data
            and not m._fivetran_deleted
            and not m.is_shadow_data
            and u.email
            !~* '.*(employmenthero|employmentinnovations|thinkei|p2m|power2motivate|test|demo|exacc|sandbox|\\+).*'
            and not u.is_shadow_data
            and not m.system_manager
            and not m.system_user
        group by 1
    ),
    -- all opportunities
    account_opp_item as (
        select distinct
            opp.account_id as external_id,
            opp.id as opportunity_id,
            opp.name as opportunity,
            opp.stage_name,
            coalesce(
                convert_timezone('Australia/Sydney', opp.demo_sat_date_c),
                convert_timezone('Australia/Sydney', opp.date_demo_booked_c)
            ) demo_date,
            convert_timezone('Australia/Sydney', cast(opp.close_date as date)) as close_date,
            su.name as opp_owner,
            convert_timezone('Australia/Sydney', cast(opp.created_date as date)) as created_date,
            oppl.name as opp_item,
            opp.opportunity_employees_c as opportunity_employees,
            oppl.quantity as item_quantity,
            opp.offer_c
        from "dev"."salesforce"."opportunity" opp
        left join "dev"."salesforce"."opportunity_line_item" oppl on opp.id = oppl.opportunity_id
        left join "dev"."salesforce"."user" su on opp.owner_id = su.id
        where not opp.is_deleted and not oppl.is_deleted
        order by opp.account_id, opp.created_date desc
    ),
    -- all won opportunities
    account_won_opp_item as (
        select
            external_id,
            opportunity,
            opportunity_id,
            opp_item,
            demo_date,
            close_date,
            opp_owner,
            created_date,
            opportunity_employees,
            item_quantity,
            offer_c,
            row_number() over (partition by external_id order by close_date desc) rn
        from account_opp_item
        where stage_name = 'Won'
    ),
    account_won_opp_item_list as (
        select
            external_id,
            len(regexp_replace(listagg(distinct opportunity_id, ','), '[^,]', '')) + 1 as won_opp_count,
            -- workaround: count(distinct opportunity_id) as won_opp_count
            '[' || listagg(
                distinct '"'
                || 'Created at '
                || created_date::date
                || ': '
                || opportunity
                || ', owner: '
                || opp_owner
                || ', closed at '
                || close_date
                || ', opportunity employees: '
                || opportunity_employees
                || '"',
                ', '
            )
            || ']' as won_opportunities,
            '[' || listagg(distinct '"' || opp_owner || '"', ', ') || ']' opp_owner,
            max(case when rn = 1 then opportunity_employees end) as opportunity_employees,
            max(case when opp_item ilike '%hr%' then item_quantity end) as opp_hr_quantity,
            max(case when opp_item ilike '%payroll%' then item_quantity end) as opp_payroll_quantity,
            '[' || listagg(distinct '"' || offer_c || '"', ', ') || ']' discounts_offered
        from account_won_opp_item
        -- where       opp_item ilike '%software%'
        -- hr software, payroll software, add-on product, implementation, etc
        group by 1
    ),
    -- most recent opportunity won
    implementation_info as (
        select
            sa.id as external_id,
            sa.name,
            ipc.service_offering_c,
            su.name as project_owner,
            ipc.name as project_name,
            coalesce(ipc.project_started_date_c, ipc.start_date_c, ipc.created_date) as kicked_off_at,
            o.close_date as opp_close_date,
            coalesce(ipc.project_completion_date_c, ipc.go_live_date_c, ipc.actual_finish_date_c) as completed_date,
            datediff(
                day, o.close_date, coalesce(ipc.project_started_date_c, ipc.start_date_c, ipc.created_date)
            ) close_to_start,
            datediff(
                day,
                coalesce(ipc.project_started_date_c, ipc.start_date_c, ipc.created_date),
                coalesce(ipc.project_completion_date_c, ipc.go_live_date_c, ipc.actual_finish_date_c)
            ) as start_to_complete
        from "dev"."salesforce"."implementation_project_c" ipc
        join "dev"."salesforce"."account" sa on sa.id = ipc.account_c
        left join account_won_opp_item_list woil on woil.external_id = sa.id and woil.won_opp_count = 1
        left join
            account_won_opp_item woi
            on woi.external_id = woil.external_id
            and (
                woi.close_date <= coalesce(ipc.project_started_date_c, ipc.start_date_c, ipc.created_date)
                or coalesce(ipc.project_started_date_c, ipc.start_date_c, ipc.created_date) is null
            )
        left join
            "dev"."salesforce"."opportunity" o
            on case when ipc.opportunity_c is not null then ipc.opportunity_c = o.id else woi.opportunity_id = o.id end
        left join "dev"."salesforce"."user" su on ipc.project_owner_c = su.id
        where not sa.is_deleted and not ipc.is_deleted and (ipc.stage_c != 'Cancelled' or ipc.stage_c is null)
    ),
    latest_completed_imp as (
        select external_id, start_to_complete as recent_start_to_complete
        from
            (
                select
                    external_id,
                    start_to_complete,
                    row_number() over (partition by external_id order by completed_date desc) rn
                from implementation_info
                where completed_date is not null
            )
        where rn = 1
    ),
    latest_started_imp as (
        select external_id, close_to_start as recent_close_to_start
        from
            (
                select
                    external_id,
                    close_to_start,
                    row_number() over (partition by external_id order by kicked_off_at desc) rn
                from implementation_info
                where kicked_off_at is not null and opp_close_date is not null
            )
        where rn = 1
    ),
    account_implementation as (
        select
            external_id,
            name,
            '[' || listagg(distinct '"' || service_offering_c || '"', ', ') || ']' as implementation,
            '[' || listagg(distinct '"' || project_owner || '"', ', ') || ']' as project_owner,
            '[' || listagg(
                distinct '"'
                || '('
                || case when project_owner is null then 'unknown' else project_owner end
                || ')'
                || project_name
                || (
                    case when kicked_off_at is null then 'not started' else ' kicked off on ' || kicked_off_at::date end
                )
                || ', '
                || (
                    case
                        when opp_close_date is null
                        then 'unknown days from close date, '
                        when kicked_off_at is null
                        then ' '
                        else close_to_start::varchar || ' days from close date, '
                    end
                )
                || (
                    case
                        when kicked_off_at is null
                        then ' '
                        when completed_date is null
                        then 'not completed'
                        else 'completed in ' || start_to_complete || ' days'
                    end
                )
                || '"',
                ', '
            ) within group (order by coalesce(kicked_off_at, getdate()) desc)
            || ']' as imp_projects,
            max(completed_date) as most_recent_project_completion_date,
            min(completed_date) as earliest_project_completion_date
        from implementation_info
        group by 1, 2
    ),
    account_imps_hr_projects as (
        select
            ipc.account_c as external_id,
            eh.org_id_c as hr_org,
            coalesce(
                ipc.project_completion_date_c, ipc.go_live_date_c, ipc.actual_finish_date_c
            ) as hr_project_completion_date
        from "dev"."salesforce"."implementation_project_c" ipc
        join "dev"."salesforce"."eh_org_c" eh on ipc.id = eh.professional_service_project_c
        where
            not ipc.is_deleted
            and not eh.is_deleted
            and (ipc.service_offering_c ilike '%hr%' or ipc.service_offering_c ilike '%combined journey%')
            and (ipc.stage_c != 'Cancelled' or ipc.stage_c is null)
    ),
    account_imps_payroll_projects as (
        select
            ipc.account_c as external_id,
            py.id_c as payroll_external_id,
            epa.organisation_id as linked_hr_org,
            coalesce(
                ipc.project_completion_date_c, ipc.go_live_date_c, ipc.actual_finish_date_c
            ) as payroll_project_completion_date
        from "dev"."salesforce"."implementation_project_c" ipc
        join "dev"."salesforce"."keypay_org_c" py on ipc.id = py.professional_service_project_c
        left join
            "dev"."postgres_public"."payroll_infos" pi on py.id_c = pi.external_id and not pi._fivetran_deleted
        left join "dev"."employment_hero"."_v_connected_payrolls" epa on pi.id = epa.payroll_info_id
        where
            not ipc.is_deleted
            and not py.is_deleted
            and (ipc.service_offering_c ilike '%payroll%' or ipc.service_offering_c ilike '%combined journey%')
            and (ipc.stage_c != 'Cancelled' or ipc.stage_c is null)
    ),
    account_demo_to_close_days as (
        select external_id, opportunity, demo_date, close_date, datediff(d, demo_date, close_date) demo_to_close_days
        from account_won_opp_item
        where rn = 1
    ),
    -- list of lost opportunities sorted by most recent close date
    account_lost_opps as (
        select
            external_id,
            '['
            || listagg(distinct '"' || 'Closed at ' || close_date || ': ' || opportunity || '"', ', ') within group (
                order by close_date desc
            )
            || ']' as lost_opportunities
        from account_opp_item
        where stage_name = 'Lost'
        group by external_id
    ),
    account_support_tickets as (
        select
            t.id,
            za.crm_id external_id,
            datediff(
                h, t.created_at, case when t.status = 'closed' then t.updated_at else null::date end
            ) resolution_time_hrs,
            t.created_at as ticket_created_at,
            case when t.status = 'closed' then t.updated_at else null::date end as ticket_closed,
            t.custom_product,
            u.name as agent,
            po.id as org_id,
            listagg(distinct '"' || replace(f.name, '::', ' > ') || '"', ', ') as ticket_tag
        from "dev"."zendesk"."ticket" t
        join "dev"."zendesk"."group" g on t.group_id = g.id
        join "dev"."zendesk"."user" u on t.assignee_id = u.id
        join "dev"."zendesk"."organization" o on t.organization_id = o.id
        join
            "dev"."postgres_public"."organisations" po
            on o.custom_organisation_id = po.id
            and not po._fivetran_deleted
            and not po.is_shadow_data
        join "dev"."zuora"."account" za on po.zuora_account_id = za.id
        -- updated to left join as some tickets don't seem to have the custom field populated
        left join
            zendesk.ticket_field_option f
            on t.custom_hr_platform_enquiry_location = f.value
            or t.custom_employment_hero_payroll_platform_product_categories = f.value
        where
            not g._fivetran_deleted
            and t.status != 'deleted'
            and g.name ~* '^support'
            -- the below conditions exclude anything that has the word support but shouldn't be included in the ticket
            -- count
            and g.name !~* 'archived|level|keypay|manager'
        group by 1, 2, 3, 4, 5, 6, 7, 8
        order by ticket_created_at desc
    ),
    account_ticket_agents_tags_product as (
        select
            external_id,
            -- ticket tags
            '[' || listagg(distinct ticket_tag, ', ') || ']' as ticket_tags,
            -- agents and resolution time
            '[' || listagg(distinct '"' || agent || '"', ', ') within group (order by agent) || ']' agents,
            avg(resolution_time_hrs) avg_resolution_time_hrs,
            -- product
            count(id) total_support_tickets,
            sum(case when custom_product = 'employment_hero_hr_platform' then 1 else 0 end) as hr_tickets,
            sum(case when custom_product = 'employment_hero_payroll_platform' then 1 else 0 end) as payroll_tickets,
            sum(
                case
                    when
                        (
                            custom_product != 'employment_hero_hr_platform'
                            and custom_product != 'employment_hero_payroll_platform'
                        )
                        or custom_product is null
                        or custom_product = ''
                    then 1
                    else 0
                end
            ) as untagged_tickets
        from account_support_tickets
        group by external_id
    ),
    account_imps_support_tickets as (
        select
            st.external_id,
            '['
            || listagg(distinct '"' || hr.hr_org || '"', ', ') within group (order by st.external_id)
            || ']' as hr_imps_org_id,
            '[' || listagg(
                distinct '"' || datediff(
                    d, case when t.product = 'hr' then hr.hr_project_completion_date end, first_support_ticket_raised
                )
                || '"',
                ', '
            ) within group (order by st.external_id)
            || ']' as first_hr_support_ticket_from_proj_completion,
            '['
            || listagg(distinct '"' || py.payroll_external_id || '"', ', ') within group (order by st.external_id)
            || ']' as payroll_imps_ext_id,
            '[' || listagg(
                distinct '"' || datediff(
                    d,
                    case when t.product = 'payroll' then py.payroll_project_completion_date end,
                    first_support_ticket_raised
                )
                || '"',
                ', '
            ) within group (order by st.external_id)
            || ']' as first_payroll_support_ticket_from_proj_completion
        from eh_hr_accounts st
        join
            (
                select
                    external_id,
                    org_id,
                    case
                        when custom_product ilike '%hr%' then 'hr' when custom_product ilike '%payroll%' then 'payroll'
                    end as product,
                    min(ticket_created_at) as first_support_ticket_raised
                from account_support_tickets
                group by 1, 2, 3
            )
            t on st.external_id = t.external_id
        left join account_imps_hr_projects hr on st.external_id = hr.external_id and t.org_id = hr.hr_org
        left join account_imps_payroll_projects py on st.external_id = py.external_id and t.org_id = py.linked_hr_org
        group by 1
    ),
    account_support_csat as (
        select
            t.external_id,
            sum(case when r.score = 'good' then 1 else 0 end) as ticket_csat_good,
            sum(case when r.score = 'bad' then 1 else 0 end) as ticket_csat_bad,
            '[' || listagg(
                distinct '"'
                || case when r.reason != 'No reason provided' then r.ticket_id || ' - ' || r.reason end
                || '"',
                ','
            )
            || ']' as ticket_csat_reason
        from account_support_tickets t
        join
            (
                select
                    ticket_id, score, reason, row_number() over (partition by ticket_id order by created_at desc) as rn
                from zendesk.satisfaction_rating
                where score not ilike 'offered'
            ) r
            on t.id = r.ticket_id
        where r.rn = 1
        group by 1
    ),
    account_feature_requests as (
        select
            t.external_id,
            count(tl.id) as total_feature_requests,
            '['
            || listagg('"' || 'Ticket: ' || tl.ticket_id || ', FR: ' || i.key || '"', ', ') within group (order by i.id)
            || ']' as feature_requests
        from account_support_tickets t
        join "dev"."zendesk"."ticket_link" tl on t.id = tl.ticket_id
        join "dev"."jira"."issue" i on tl.issue_id = i.id
        where not tl._fivetran_deleted and not i._fivetran_deleted
        group by t.external_id
    ),
    account_primary_contact as (
        select
            c.account_id as external_id,
            '[' || listagg(distinct '"' || c.id || '"', ', ') within group (order by c.email) || ']' as sf_contact_id,
            '['
            || listagg(distinct '"' || u.uuid || '"', ', ') within group (order by c.email)
            || ']' as sf_contact_user_uuid,
            '['
            || listagg(distinct '"' || c.first_name || ' ' || c.last_name || '"', ', ') within group (order by c.email)
            || ']' as primary_contact,
            '[' || listagg(distinct '"' || c.email || '"', ', ') within group (order by c.email) || ']' as sf_email
        from "dev"."salesforce"."contact" c
        join "dev"."zuora"."account" za on c.account_id = za.crm_id
        join "dev"."postgres_public"."organisations" o on za.id = o.zuora_account_id
        join "dev"."postgres_public"."members" m on o.id = m.organisation_id
        join "dev"."postgres_public"."users" u on m.user_id = u.id
        where
            not c.is_deleted
            and c.primary_contact_c
            and not c.no_longer_with_company_c
            and not o._fivetran_deleted
            and not o.is_shadow_data
            and not u._fivetran_deleted
            and not u.is_shadow_data
            and not za._fivetran_deleted
            and not m._fivetran_deleted
            and not m.is_shadow_data
            and c.email = u.email
            and u.email
            !~* '.*(employmenthero|employmentinnovations|thinkei|p2m|power2motivate|test|demo|exacc|sandbox|\\+).*'
            and not m.system_manager
            and not m.system_user
            and not m.independent_contractor
        group by c.account_id
    ),
    account_billing_contact as (
        select
            account_id as external_id,
            '[' || listagg(distinct '"' || id || '"', ', ') within group (order by email) || ']' as sf_contact_id,
            '['
            || listagg(distinct '"' || first_name || ' ' || last_name || '"', ', ') within group (order by email)
            || ']' as billing_contact,
            '[' || listagg(distinct '"' || email || '"', ', ') within group (order by email) || ']' as sf_email
        from "dev"."salesforce"."contact"
        where not is_deleted and billing_contact_c and not no_longer_with_company_c
        group by account_id
    ),
    account_benefits as (
        select sa.id as external_id, sum(savings) as employee_savings
        from "dev"."salesforce"."account" sa
        join "dev"."zuora"."account" za on za.crm_id = sa.id
        left join
            "dev"."postgres_public"."organisations" o
            on za.id = o.zuora_account_id
            and not o._fivetran_deleted
            and not o.is_shadow_data
        left join
            -- [heroshop_orders as ho]
            (
                with
                    product_variants2 as (
                        select
                            *,
                            (
                                case
                                    when supplier_price is not null
                                    then supplier_price
                                    when variant_code ~* '-50'
                                    then 47.37
                                    when variant_code !~* 'EMOVIE|ESAVER'
                                    then 94.74
                                    when variant_code = 'ESAVER_CHILD'
                                    then 10.5
                                    when variant_code = 'EMOVIE_ADULT'
                                    then 15.5
                                    when variant_code = 'ESAVER_ADULT'
                                    then 12.5
                                    when variant_code = 'EMOVIE_CHILD'
                                    then 12.5
                                    else 0
                                end
                            ) as supplier_cost
                        from "dev"."heroshop_db_public"."product_variants"
                    )

                select
                    od.id,
                    od.created_at,
                    m.id as member_id,
                    m.user_id as user_id,
                    m.organisation_id,
                    case
                        when t.payment_method = 1
                        then 'Instapay'
                        when t.payment_method = 2
                        then 'HeroDollars'
                        else 'Credit Card'
                    end as payment_method,
                    p.name,
                    pc.name as product_category,
                    od.quantity,
                    pv.variant_code,
                    pv.supplier_cost * quantity as supplier_price,
                    od.price * quantity as price,
                    od.discount,
                    od.transaction_fee,
                    od.freight_cost,
                    od.billable_amount,
                    od.discount - od.transaction_fee as savings,
                    od.billable_amount - (quantity * pv.supplier_cost) - od.freight_cost - od.transaction_fee as margin,
                    od.status
                from "dev"."heroshop_db_public"."order_details" od
                join product_variants2 pv on pv.id = od.product_variant_id
                join "dev"."heroshop_db_public"."orders" o on od.order_id = o.id
                join "dev"."heroshop_db_public"."products" p on p.id = pv.product_id
                join
                    "dev"."postgres_public"."members" m
                    on m.uuid = o.member_id
                    and not m._fivetran_deleted
                    and not m.is_shadow_data
                join
                    "dev"."postgres_public"."organisations" org
                    on m.organisation_id = org.id
                    and not org._fivetran_deleted
                    and not org.is_shadow_data
                join "dev"."heroshop_db_public"."transactions" t on o.id = t.order_id
                join "dev"."heroshop_db_public"."product_categories" pc on p.product_category_id = pc.id
                where org.name !~* 'Winterfell Trading Name|KevTest' and od.status !~* 'cancel|refund|declined|failed'
                order by created_at desc
            ) ho
            on ho.organisation_id = o.id
        group by 1
    ),
    account_herodollar_balance as (
        select sa.id as external_id, sum(hd_quantity) as herodollar_balance
        from "dev"."salesforce"."account" sa
        join "dev"."zuora"."account" za on za.crm_id = sa.id
        left join
            "dev"."postgres_public"."organisations" o
            on za.id = o.zuora_account_id
            and not o._fivetran_deleted
            and not o.is_shadow_data
        left join
            -- [corporate_herodollars as cho]
            (
                with
                    imps_incentive as (
                        select transactable_id
                        from "dev"."herodollar_service_public"."hero_dollar_transactions"
                        join
                            "dev"."herodollar_service_public"."tracking_infos"
                            on hero_dollar_transactions.id = tracking_infos.hero_dollar_transaction_id
                        where
                            hero_dollar_transactions.transactable_type = 'Organisation'
                            and tracking_infos.reason_type = 3
                    )
                select distinct org_id, hd_quantity
                from
                    (
                        select o.id as org_id, sum(t.amount) as hd_quantity
                        from "dev"."herodollar_service_public"."hero_dollar_transactions" t
                        join
                            "dev"."postgres_public"."organisations" o
                            on t.transactable_id = o.uuid
                            and not o._fivetran_deleted
                            and not o.is_shadow_data
                        where t.transactable_type = 'Organisation'
                        group by o.id
                    ) h
            ) cho
            on cho.org_id = o.id
        group by 1
    ),
    account_imps_payroll_days as (
        select distinct
            imp.account_c as external_id,
            '[' || listagg(
                distinct '"' || case
                    when imp.service_offering_c ~* '(Guided Payroll|combined journey)'
                    then datediff(d, t.completed_at, pr.completed_at)
                end
                || '"',
                ', '
            )
            || ']' as gp_kickoff_to_project_completed,
            '[' || listagg(
                distinct '"' || case
                    when imp.service_offering_c ilike '%Managed Payroll%'
                    then datediff(d, t.completed_at, pr.completed_at)
                end
                || '"',
                ', '
            )
            || ']' as mp_kickoff_to_live_payrun
        from "dev"."asana"."task" t
        join "dev"."asana"."project_task" pt on t.id = pt.task_id
        join
            "dev"."salesforce"."asana_public_asana_projects_relation_c" ap
            on pt.project_id = ap.asana_public_asana_project_id_c
        join "dev"."salesforce"."implementation_project_c" imp on ap.asana_public_object_id_c = imp.id
        join
            (
                select pt.project_id, t.completed_at
                from "dev"."asana"."task" t
                join "dev"."asana"."project_task" pt on t.id = pt.task_id
                where
                    not t._fivetran_deleted
                    and t.completed
                    and (t.name = 'Project Complete - GP' or t.name = 'Support live pay run')
            ) pr
            on pt.project_id = pr.project_id
        where
            not imp.is_deleted
            and not t._fivetran_deleted
            and (t.name = 'Kick off email - GP' or t.name = 'Conduct Kick-off call')
            and t.completed
        group by imp.account_c
    ),
    accounts_zuora_payments as (
        select
            external_id,
            count(case when payment_status = 'Processed' then payment_id end) as total_paid_invoices,
            avg(days_to_make_payments) avg_days_to_make_payments,
            count(case when payment_status = 'Error' then payment_id end) as total_late_payments
        from
            (
                select distinct
                    a.crm_id as external_id,
                    p.id as payment_id,
                    p.status as payment_status,
                    case
                        when p.effective_date >= i.posted_date and p.status = 'Processed'
                        then datediff(d, i.posted_date, p.effective_date)
                        when p.effective_date < i.posted_date and p.status = 'Processed'
                        then datediff(d, i.invoice_date, p.effective_date)
                    end as days_to_make_payments
                from "dev"."zuora"."account" a
                join "dev"."zuora"."invoice" i on a.id = i.account_id
                join "dev"."zuora"."payment_application" pa on i.id = pa.invoice_id
                join "dev"."zuora"."payment" p on pa.payment_id = p.id
                where
                    not a._fivetran_deleted
                    and not i._fivetran_deleted
                    and i.status = 'Posted'
                    and not pa._fivetran_deleted
                    and not p._fivetran_deleted
                order by p.effective_date desc, i.posted_date desc
            )
        where external_id is not null
        group by 1
    ),
    ipc_org as (
        select
            ipc.id as project_id,
            ipc.account_c as external_id,
            so.org_id_c as org_id,
            listagg(o.id, ',') as account_orgs,
            count(o.id) as org_count
        from "dev"."salesforce"."implementation_project_c" ipc
        join "dev"."zuora"."account" za on za.crm_id = ipc.account_c
        left join
            "dev"."salesforce"."eh_org_c" so
            on so.professional_service_project_c = ipc.id
            and not so.is_deleted
        left join
            "dev"."postgres_public"."organisations" o
            on za.id = o.zuora_account_id
            and not o._fivetran_deleted
            and not o.is_shadow_data
        where not ipc.is_deleted and not za._fivetran_deleted
        group by 1, 2, 3
    ),
    happiness_surveys as (
        select h.organisation_id, ipc_org.project_id, min(h.updated_at) as happiness_survey_at
        from "dev"."survey_services_public"."happiness_surveys" as h
        left join
            "dev"."postgres_public"."organisations" o
            on h.organisation_id = o.uuid
            and not o._fivetran_deleted
            and not o.is_shadow_data
        left join ipc_org on ipc_org.org_id = o.id or (ipc_org.org_count = 1 and ipc_org.account_orgs = o.id)
        where not h._fivetran_deleted
        group by 1, 2
    ),
    account_managed_hr_milestones as (
        select distinct
            imp.account_c as external_id,
            '['
            || listagg(distinct '"' || t.name || '"', ', ')
            || (case when hs.happiness_survey_at is not null then ', "Happiness survey"' else '' end)
            || ']' as completed_managed_hr_milestones
        from "dev"."asana"."task" t
        join "dev"."asana"."project_task" pt on t.id = pt.task_id
        join
            "dev"."salesforce"."asana_public_asana_projects_relation_c" ap
            on pt.project_id = ap.asana_public_asana_project_id_c
        join "dev"."salesforce"."implementation_project_c" imp on ap.asana_public_object_id_c = imp.id
        left join happiness_surveys hs on hs.project_id = imp.id
        where
            not imp.is_deleted
            and imp.service_offering_c ilike '%Managed HR%'
            and not t._fivetran_deleted
            and (
                t.name = 'HR - System Integration (if applicable)'
                or t.name = 'HR - Customer 1:1'
                or t.name = 'Setup Company Values and Branding'
                or t.name = 'Import Employee Data'
                or t.name = 'Setup Licences and Certifications'
                or t.name = 'Build Policies'
                or t.name = 'Setup Onboarding Checklists'
            )
            -- t.name ~* '(HR - System Integration (if applicable)|HR - Customer 1:1|Setup Company Values and
            -- Branding|Import Employee Data|Setup Licences and Certifications|Build Policies|Setup Onboarding
            -- Checklists)'
            and t.completed
        group by 1, hs.happiness_survey_at
    ),
    account_guided_hr_milestones as (
        select distinct
            imp.account_c as external_id,
            '['
            || listagg(distinct '"' || t.custom_feature || '"', ', ')
            || listagg(distinct case when hs.happiness_survey_at is not null then ', "Happiness survey"' else '' end)
            || ']' as completed_guided_hr_milestones
        from "dev"."asana"."task" t
        join "dev"."asana"."project_task" pt on t.id = pt.task_id
        join
            "dev"."salesforce"."asana_public_asana_projects_relation_c" ap
            on pt.project_id = ap.asana_public_asana_project_id_c
        join "dev"."salesforce"."implementation_project_c" imp on ap.asana_public_object_id_c = imp.id
        left join happiness_surveys hs on hs.project_id = imp.id
        where
            not imp.is_deleted
            and imp.service_offering_c ~* '(Guided HR|combined journey)'
            and not t._fivetran_deleted
            and t.name like 'Recommended Milestone %'
            -- only check the first 4 milestones for eligibility
            and t.completed
        group by 1
    ),
    account_guided_hr_milestones_addons as (
        select
            imp.account_c as external_id,
            listagg(
                datediff(d, convert_timezone('Australia/Sydney', t.completed_at), i.invitation_date), ', '
            ) as ghr_kickoff_to_invitation,
            listagg(
                datediff(d, convert_timezone('Australia/Sydney', t.completed_at), i.live_date), ', '
            ) as ghr_kickoff_to_live_date
        from "dev"."asana"."task" t
        join "dev"."asana"."project_task" pt on t.id = pt.task_id
        join
            "dev"."salesforce"."asana_public_asana_projects_relation_c" ap
            on pt.project_id = ap.asana_public_asana_project_id_c
        join "dev"."salesforce"."implementation_project_c" imp on ap.asana_public_object_id_c = imp.id
        join
            (
                select
                    pt.project_id,
                    case
                        when t.name = 'Invite Employees' then convert_timezone('Australia/Sydney', t.completed_at)
                    end invitation_date,
                    case when t.name = 'HR-Live' then convert_timezone('Australia/Sydney', t.completed_at) end live_date
                from "dev"."asana"."task" t
                join "dev"."asana"."project_task" pt on t.id = pt.task_id
                where not t._fivetran_deleted and t.completed and t.name in ('Invite Employees', 'HR-Live')
            ) i
            on pt.project_id = i.project_id
        where
            not imp.is_deleted
            and imp.service_offering_c ~* '%(Guided HR|combined journey)%'
            and not t._fivetran_deleted
            and t.completed
            and t.name = 'Kick off email - HR'
        group by 1
    ),
    account_payroll_milestones as (
        select distinct
            imp.account_c as external_id,
            '[' || listagg(
                distinct '"' || case
                    when
                        imp.service_offering_c ~* '(Guided Payroll|combined journey)' and t.name = 'Kick off email - GP'
                    then 'Project kicked-off'
                    when
                        imp.service_offering_c ~* '(Guided Payroll|combined journey)'
                        and t.name = 'Project Complete - GP'
                    then 'Project completed'
                end
                || '"',
                ', '
            ) within group (order by t.completed_at)
            || ']' as completed_guided_payroll_milestones,
            '[' || listagg(
                distinct '"' || case
                    when imp.service_offering_c ilike '%Managed Payroll%' and t.name = 'Import Employee Data'
                    then 'Employee data imported'
                    when imp.service_offering_c ilike '%Managed Payroll%' and t.name = 'Conduct Scenario UAT'
                    then 'Scenario testing signed off'
                    when imp.service_offering_c ilike '%Managed Payroll%' and t.name = 'Conduct platform walkthrough'
                    then 'System walkthrough completed'
                    when imp.service_offering_c ilike '%Managed Payroll%' and t.name = 'Conduct systems sync'
                    then 'Systems synced'
                    when imp.service_offering_c ilike '%Managed Payroll%' and t.name = 'Schedule live pay run'
                    then 'Scheduled live pay run'
                    when imp.service_offering_c ilike '%Managed Payroll%' and t.name = 'Customer Testing complete'
                    then 'Client testing completed'
                    when imp.service_offering_c ilike '%Managed Payroll%' and t.name = 'Support live pay run'
                    then 'Support live pay run completed'
                end
                || '"',
                ', '
            ) within group (order by t.completed_at)
            || ']' as completed_managed_payroll_milestones
        from "dev"."asana"."task" t
        join "dev"."asana"."project_task" pt on t.id = pt.task_id
        join
            "dev"."salesforce"."asana_public_asana_projects_relation_c" ap
            on pt.project_id = ap.asana_public_asana_project_id_c
        join "dev"."salesforce"."implementation_project_c" imp on ap.asana_public_object_id_c = imp.id
        where
            not imp.is_deleted
            and not t._fivetran_deleted
            -- guided payroll
            and (
                t.name = 'Kick off email - GP'
                or t.name = 'Project Complete - GP'
                -- managed payroll
                or t.name = 'Import Employee Data'
                or t.name = 'Conduct Scenario UAT'
                or t.name = 'Conduct platform walkthrough'
                or t.name = 'Conduct systems sync'
                or t.name = 'Schedule live pay run'
                or t.name = 'Customer Testing complete'
                or t.name = 'Support live pay run'
            )
            and t.completed
        group by 1
    ),
    account_payroll_platform as (
        select
            za.crm_id as external_id,
            '[' || listagg(
                distinct '"'
                || coalesce(epa.connected_app, substring(epa.type, 1, charindex('Auth', epa.type) - 1))
                || '"',
                ', '
            )
            || ']' as payroll_platform,
            '['
            || listagg('"' || pi.id || '"', ', ') within group (order by epa.created_at)
            || ']' as payroll_external_id,
            '[' || listagg(
                distinct '"' || case
                    when json_extract_path_text(epa.data, 'kp_white_label') != ''
                    then json_extract_path_text(epa.data, 'kp_white_label')
                end
                || '"',
                ', '
            )
            || ']' as white_label
        from "dev"."zuora"."account" za
        join "dev"."postgres_public"."organisations" o on za.id = o.zuora_account_id
        join "dev"."employment_hero"."_v_connected_payrolls" epa on o.id = epa.organisation_id
        join "dev"."postgres_public"."payroll_infos" pi on epa.payroll_info_id = pi.id
        where
            not za._fivetran_deleted
            and not o._fivetran_deleted
            and not o.is_shadow_data
            and not epa._fivetran_deleted
            and not pi._fivetran_deleted
            and pi.status = 1
        group by 1
    ),
    account_proserv_csat as (
        select
            external_id,
            max(proserv_csat_kick_off_hr) as proserv_csat_kick_off_hr,
            max(proserv_csat_kick_off_payroll) as proserv_csat_kick_off_payroll,
            max(proserv_csat_midway_hr) as proserv_csat_midway_hr,
            max(proserv_csat_midway_payroll) as proserv_csat_midway_payroll,
            max(proserv_csat_completion_hr) as proserv_csat_completion_hr,
            max(proserv_csat_completion_payroll) as proserv_csat_completion_payroll
        from
            (
                select
                    implementation_project_c.account_c as external_id,
                    kick_off_hr.score as proserv_csat_kick_off_hr,
                    kick_off_payroll.score as proserv_csat_kick_off_payroll,
                    midway_hr.score as proserv_csat_midway_hr,
                    midway_payroll.score as proserv_csat_midway_payroll,
                    completion_hr.score as proserv_csat_completion_hr,
                    completion_payroll.score as proserv_csat_completion_payroll,
                    case
                        when
                            coalesce(
                                kick_off_hr.properties_service,
                                midway_hr.properties_service,
                                completion_hr.properties_service
                            )
                            ilike '%hr%'
                        then 'hr'
                        else 'payroll'
                    end as service_offering,
                    coalesce(
                        kick_off_hr.created_at,
                        kick_off_payroll.created_at,
                        midway_hr.created_at,
                        midway_payroll.created_at,
                        completion_hr.created_at,
                        completion_payroll.created_at
                    ) as response_created_at,
                    -- get the most recent score based on SF account, CSAT phase and HR/Payroll
                    row_number() over (
                        partition by
                            external_id,
                            coalesce(
                                kick_off_hr.properties_phase,
                                midway_hr.properties_phase,
                                completion_hr.properties_phase,
                                kick_off_payroll.properties_phase,
                                midway_payroll.properties_phase,
                                completion_payroll.properties_phase
                            ),
                            service_offering
                        order by response_created_at desc
                    ) as rn,
                    coalesce(
                        kick_off_hr.permalink,
                        kick_off_payroll.permalink,
                        midway_hr.permalink,
                        midway_payroll.permalink,
                        completion_hr.permalink,
                        completion_payroll.permalink
                    ) as permalink
                from "dev"."salesforce"."implementation_project_c"
                join
                    "dev"."salesforce"."asana_public_asana_projects_relation_c" ap
                    on ap.asana_public_object_id_c = implementation_project_c.id
                left join
                    "dev"."delighted_proserv_csat"."response" as kick_off_hr
                    on kick_off_hr.properties_phase = 'Kick-off'
                    and kick_off_hr.properties_service ilike '%hr%'
                    and kick_off_hr.properties_project_id = ap.asana_public_asana_project_id_c
                left join
                    "dev"."delighted_proserv_csat"."response" as kick_off_payroll
                    on kick_off_payroll.properties_phase = 'Kick-off'
                    and kick_off_payroll.properties_service ilike '%payroll%'
                    and kick_off_payroll.properties_project_id = ap.asana_public_asana_project_id_c
                left join
                    "dev"."delighted_proserv_csat"."response" as midway_hr
                    on midway_hr.properties_phase = 'Midway'
                    and midway_hr.properties_service ilike '%hr%'
                    and midway_hr.properties_project_id = ap.asana_public_asana_project_id_c
                left join
                    "dev"."delighted_proserv_csat"."response" as midway_payroll
                    on midway_payroll.properties_phase = 'Midway'
                    and midway_payroll.properties_service ilike '%payroll%'
                    and midway_payroll.properties_project_id = ap.asana_public_asana_project_id_c
                left join
                    "dev"."delighted_proserv_csat"."response" as completion_hr
                    on completion_hr.properties_phase = 'Completion'
                    and completion_hr.properties_service ilike '%hr%'
                    and completion_hr.properties_project_id = ap.asana_public_asana_project_id_c
                left join
                    "dev"."delighted_proserv_csat"."response" as completion_payroll
                    on completion_payroll.properties_phase = 'Completion'
                    and completion_payroll.properties_service ilike '%payroll%'
                    and completion_payroll.properties_project_id = ap.asana_public_asana_project_id_c
                where
                    not ap.is_deleted
                    and not implementation_project_c.is_deleted
                    and coalesce(
                        kick_off_hr.properties_delighted_source,
                        midway_hr.properties_delighted_source,
                        completion_hr.properties_delighted_source,
                        kick_off_payroll.properties_delighted_source,
                        midway_payroll.properties_delighted_source,
                        completion_payroll.properties_delighted_source
                    )
                    = 'Email'
            )
        where rn = 1
        group by 1
    ),
    account_owner as (
        select external_id, user_uuid
        from
            (
                select
                    za.crm_id as external_id,
                    m.organisation_id,
                    m.role,
                    m.first_name || ' ' || m.last_name as name,
                    u.email,
                    u.uuid as user_uuid
                from "dev"."postgres_public"."members" as m
                join "dev"."postgres_public"."users" as u on u.id = m.user_id
                join "dev"."postgres_public"."organisations" as org on m.organisation_id = org.id
                join "dev"."zuora"."account" as za on org.zuora_account_id = za.id
                where
                    m.id in (
                        select
                            first_value(m.id) over (
                                partition by za.crm_id
                                order by m.role desc, m.created_at asc
                                rows between unbounded preceding and current row
                            )
                        from "dev"."postgres_public"."members" as m
                        join "dev"."postgres_public"."users" as u on u.id = m.user_id
                        join "dev"."postgres_public"."organisations" as org on m.organisation_id = org.id
                        join "dev"."zuora"."account" as za on org.zuora_account_id = za.id
                        where
                            m.role != 'employee'
                            and m.active
                            and m.accepted
                            and u.email
                            !~* '.*(employmenthero|employmentinnovations|thinkei|p2m|power2motivate|test|demo|exacc|sandbox|\\+).*'
                            and not u._fivetran_deleted
                            and not u.is_shadow_data
                            and not m.system_manager
                            and not m.system_user
                            and not m.independent_contractor
                            and not m._fivetran_deleted
                            and not m.is_shadow_data
                            and not org._fivetran_deleted
                            and not org.is_shadow_data
                            and not za._fivetran_deleted
                    )
                    and not m._fivetran_deleted
                    and not m.is_shadow_data
                    and not u._fivetran_deleted
                    and not u.is_shadow_data
                    and not org._fivetran_deleted
                    and not org.is_shadow_data
                    and not za._fivetran_deleted
            )
        where external_id is not null
    ),
    account_sales_csat as (
        select opp.account_id as external_id, response.score as sales_csat_score
        from "dev"."delighted_sales_csat"."response"
        join "dev"."salesforce"."opportunity" as opp on response.properties_opportunity_id = opp.id
        where
            not opp.is_deleted
            and response.id in (
                select
                    first_value(r.id) over (
                        partition by opp.account_id
                        order by created_at desc
                        rows between unbounded preceding and unbounded following
                    )
                from "dev"."delighted_sales_csat"."response" r
                join "dev"."salesforce"."opportunity" as opp on r.properties_opportunity_id = opp.id
            )
    )

select distinct
    st.external_id,
    st.name,
    st.account_stage,
    st.churn_date,
    st.hr_org_id,
    case
        when pp.payroll_platform is null and po.payroll_org_id is not null
        then 'Employment Hero Payroll'
        else pp.payroll_platform
    end as payroll_platform,
    pp.white_label,
    nvl(pp.payroll_external_id, po.payroll_org_id) as payroll_external_id,
    bd.billing_account_number,
    bd.zuora_geo as country,
    bd.created_date,
    bd.industry,
    em.active_employees,
    em.pending_employees,
    em.active_and_pending_employees,
    em.independent_contractors,
    em.terminated_employees,
    s.subscription,
    cd.service_activation_date,
    cd.term_end_date,
    cmr.cmrr,
    mr.mrr,
    cmr.outstanding_balance,
    t.total_support_tickets as support_total_tickets,
    t.total_support_tickets / nullif(em.active_employees, 0) as support_ticket_active_employee_ratio,
    t.hr_tickets as support_hr_tickets,
    t.payroll_tickets as support_payroll_tickets,
    t.untagged_tickets as support_untagged_tickets,
    t.ticket_tags as support_ticket_tags,
    t.avg_resolution_time_hrs as support_avg_resolution_time_hrs,
    t.agents as support_agents,
    demo.opportunity as most_recent_won_opp,
    demo.demo_date,
    demo.close_date,
    demo.demo_to_close_days,
    lost.lost_opportunities,
    wol.won_opportunities,
    wol.opportunity_employees,
    wol.opp_owner,
    wol.opp_hr_quantity,
    wol.opp_payroll_quantity,
    im.implementation,
    im.imp_projects,
    im.project_owner,
    im.earliest_project_completion_date as earliest_go_live_date,
    im.most_recent_project_completion_date as latest_go_live_date,
    lsi.recent_close_to_start,
    lci.recent_start_to_complete,
    dma.daily_users as dau,
    dma.monthly_users as mau,
    dma.dau_mau,
    fr.total_feature_requests,
    fr.feature_requests,
    pc.sf_contact_user_uuid as primary_contact_uuid,
    pc.primary_contact,
    pc.sf_email as primary_contact_email,
    bc.billing_contact,
    bc.sf_email as billing_contact_email,
    ab.employee_savings,
    hb.herodollar_balance,
    wol.discounts_offered,
    af.currency as billing_currency,
    af.estimated_minimum_users_hr,
    af.list_price_per_unit_hr,
    af.discount_price_per_unit_hr,
    af.estimated_minimum_users_payroll,
    af.list_price_per_unit_payroll,
    af.discount_price_per_unit_payroll,
    af.estimated_minimum_users_addon,
    af.list_price_per_unit_addon,
    af.discount_price_per_unit_addon,
    zp.avg_days_to_make_payments,
    zp.total_late_payments,
    amhm.completed_managed_hr_milestones,
    aghm.completed_guided_hr_milestones,
    aghma.ghr_kickoff_to_invitation,
    aghma.ghr_kickoff_to_live_date,
    ipd.mp_kickoff_to_live_payrun,
    pm.completed_managed_payroll_milestones,
    ipd.gp_kickoff_to_project_completed,
    pm.completed_guided_payroll_milestones,
    ist.hr_imps_org_id,
    ist.first_hr_support_ticket_from_proj_completion,
    ist.payroll_imps_ext_id,
    ist.first_payroll_support_ticket_from_proj_completion,
    sc.ticket_csat_good,
    sc.ticket_csat_bad,
    sc.ticket_csat_reason,
    proserv.proserv_csat_kick_off_hr,
    proserv.proserv_csat_kick_off_payroll,
    proserv.proserv_csat_midway_hr,
    proserv.proserv_csat_midway_payroll,
    proserv.proserv_csat_completion_hr,
    proserv.proserv_csat_completion_payroll,
    convert_timezone('Australia/Sydney', getdate()) as _fivetran_transformed,
    mr.invoice_month,
    ao.user_uuid,
    sales.sales_csat_score,
    po.setup_mode_org_id,
    st.business_account_name
from account_stages st
left join account_basic_details bd on st.external_id = bd.external_id
left join account_implementation im on st.external_id = im.external_id
left join latest_started_imp lsi on st.external_id = lsi.external_id
left join latest_completed_imp lci on st.external_id = lci.external_id
left join account_cmrr cmr on st.external_id = cmr.external_id
left join account_mrr mr on st.external_id = mr.external_id
left join subscription s on st.external_id = s.external_id
left join account_finance af on st.external_id = af.external_id
left join account_employees em on st.external_id = em.external_id
left join account_demo_to_close_days demo on st.external_id = demo.external_id
left join account_lost_opps lost on st.external_id = lost.external_id  -- left join account_benefits ab on
left join account_ticket_agents_tags_product t on st.external_id = t.external_id
left join account_won_opp_item_list wol on st.external_id = wol.external_id
left join account_feature_requests fr on st.external_id = fr.external_id
left join account_primary_contact pc on st.external_id = pc.external_id
left join account_billing_contact bc on st.external_id = bc.external_id
left join account_benefits ab on st.external_id = ab.external_id
left join account_herodollar_balance hb on st.external_id = hb.external_id
left join accounts_zuora_payments zp on st.external_id = zp.external_id
left join account_managed_hr_milestones amhm on st.external_id = amhm.external_id
left join account_guided_hr_milestones aghm on st.external_id = aghm.external_id
left join account_guided_hr_milestones_addons aghma on st.external_id = aghma.external_id
left join account_payroll_milestones pm on st.external_id = pm.external_id
left join account_imps_payroll_days ipd on st.external_id = ipd.external_id
left join account_imps_support_tickets ist on st.external_id = ist.external_id
left join account_support_csat sc on st.external_id = sc.external_id
left join
    "dev"."mp"."daumau_by_account" dma
    on dma.account_id = st.external_id
    and dma.date = (select max(date) from "dev"."mp"."daumau_by_account")
left join account_payroll_platform pp on st.external_id = pp.external_id
left join stage_completed po on st.external_id = po.external_id  -- payroll only orgs
left join account_contract_details cd on st.external_id = cd.external_id
left join account_proserv_csat proserv on st.external_id = proserv.external_id
left join account_owner ao on st.external_id = ao.external_id
left join account_sales_csat as sales on st.external_id = sales.external_id