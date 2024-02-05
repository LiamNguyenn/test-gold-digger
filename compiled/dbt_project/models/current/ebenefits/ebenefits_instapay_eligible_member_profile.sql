

with 
    payslips as (
        select member_id      
        , datediff('week', pay_period_starting, pay_period_ending) as weeks
        , case
            when weeks < 2 then 'Weekly'
            when weeks = 2 then 'Fortnightly'
            when weeks between 3 and 6 then 'Monthly'
            when weeks between 12 and 13 then 'Quarterly'
            when weeks = 26 then 'Biannually'
            when weeks = 52 then 'Annually'
        end pay_frequency 
        from 
            "dev"."postgres_public"."payslips" ps
        where 
            not ps._fivetran_deleted
            and id in (  
                select
                    FIRST_VALUE(id) over(partition by member_id order by pay_period_ending desc rows between unbounded preceding and unbounded following)
                from
                    "dev"."postgres_public"."payslips" ps
                where
                    not _fivetran_deleted
                    and pay_period_ending < getdate()
                )
    )
    , salary_version AS (
        select
            sv.*
            , initcap(ps.frequency) as frequency
            , case when salary_type ~* 'hour' and hours_per_week > 0 then salary * hours_per_week * 52
                when salary_type ~* 'day' and days_per_week > 0 then salary * days_per_week * 52
                when salary_type ~* 'fortnight' and days_per_week > 0 then salary * days_per_week * 26
                when salary_type ~* '^month' then salary * 12 
                when salary_type ~* 'annum' then salary
                else 0 end as yearly_salary
        from 
            

(
select
    *
  from
    "dev"."postgres_public"."salary_versions"
  where
    id in (
      select
        FIRST_VALUE(id) over(partition by member_id order by created_at desc rows between unbounded preceding and unbounded following)
      from
        "dev"."postgres_public"."salary_versions"
      where
        not _fivetran_deleted
    )
)

 as sv
            left join "dev"."postgres_public"."pay_schedules" as ps on
                sv.pay_schedule_id = ps.id
        where 
            not sv.in_review
            and (sv.effective_from <= current_date or sv.effective_from is null)
            and not sv._fivetran_deleted
            and not ps._fivetran_deleted
    )
    , unlaunched_organisations AS (
        select
            organisation_id
        from
            "dev"."postgres_public"."sandbox_settings"
        where
            organisation_id is not null
            and not sandbox_settings._fivetran_deleted
    )
    , heropay_transactions_by_member AS (
        select
            member_id AS member_uuid
            ,sum(coalesce(admin_fee, 0)) as total_admin_fee
            ,count(id) as total_heropay_transactions
            ,min(created_at) as first_created_at
            ,max(created_at) as last_created_at
        from
            "dev"."heropay_db_public"."heropay_transactions"
        where 
            not _fivetran_deleted or _fivetran_deleted is null
        group by member_id
    )
    , instapay_members as (
        select
            m.id as member_id
            , m.user_id
            , m.user_uuid
            , lower(m.email) as email
            , m.created_at
            , m.active
            , m.start_date
            , m.termination_date
            , datediff('year', m.date_of_birth, getdate()) as age
            , case when m.gender ~* '^f' then 'Female'
                when m.gender ~* '^m' then 'Male' 
                else 'N/A' end as gender
            , case when age < 18 THEN ' <18'
                WHEN age >= 18 AND age <= 24 THEN '18-24'
                WHEN age > 24 AND age <= 34 THEN '25-34'
                WHEN age > 34 AND age <= 44 THEN '35-44'
                WHEN age > 44 AND age <= 54 THEN '45-54'
                WHEN age > 54 AND age <= 64 THEN '55-64'
                WHEN age > 64 THEN '65+' end as age_bracket 
            , case
                when ea.state ~* '(South Australia|SA)' then 'SA'
                when ea.state ~* '(Northern Territory|NT)' then 'NT'
                when ea.state ~* '(Victoria|VIC)' then 'VIC'
                when ea.state ~* '(New South|NSW)' then 'NSW'
                when ea.state ~* '(Queensland|QLD)' then 'QLD'
                when ea.state ~* '(Tasmania|TAS)' then 'TAS'
                when ea.state ~* '(Western Australia|WA)' then 'WA'
                when ea.state ~* '(Australian Capital Territory|ACT)' then 'ACT'
                else null end as residential_state
            , m.latest_employment_type as employment_type
            , m.organisation_id
            , o.name as organisation_name
            , case when os.active_employees < 20 then '1-19'
                when os.active_employees >= 20 and os.active_employees < 200 then '20-199' 
                when os.active_employees >= 200 then '200+' end as business_size
            , oi.consolidated_industry as industry 
            , o.payroll_type
            , o.connected_app
            , sv.salary_type
            , coalesce(sv.frequency,ps.pay_frequency) as pay_frequency

            , coalesce(mp.monthly_wages*12, sv.yearly_salary)as annum_salary 
            , case when annum_salary/1000 < 40 then '<40k'
                when annum_salary/1000 >= 40 and annum_salary/1000 < 50 then '40-50k'
                when annum_salary/1000 >= 50 and annum_salary/1000 < 70 then '50-70k'
                when annum_salary/1000 >= 70 and annum_salary/1000 < 100 then '70-100k'
                when annum_salary/1000 >= 100 and annum_salary/1000 < 120 then '100-120k' 
                when annum_salary/1000 >= 120 and annum_salary/1000 < 160 then '120-160k' 
                when annum_salary/1000 >= 160 then '160k+' end as income_bracket

            ,hp.total_heropay_transactions
            ,hp.total_admin_fee as revenue
            ,hp.first_created_at::date as first_time_instapay_usage
            ,hp.last_created_at::date as last_time_instapay_usage
            ,sa.first_time_swag_app
        from 
            "dev"."employment_hero"."employees" as m
            join "dev"."postgres_public"."payroll_infos" as i on 
                i.id = m.payroll_info_id
                and i.status = 1
                and not i._fivetran_deleted
            join salary_version sv on
                m.id = sv.member_id
                and sv.salary_type in ('Hour', 'Annum')
            left join payslips ps on 
                ps.member_id = m.id
            left join 
                (select distinct 
                    member_id
                    , FIRST_VALUE(monthly_wages) over (partition by member_id order by month desc rows between unbounded preceding and unbounded following) as monthly_wages
                from employment_hero.au_employee_monthly_pay
                ) mp on 
                mp.member_id = m.id
            left join "dev"."postgres_public"."addresses" ea on 
                m.address_id = ea.id and not ea._fivetran_deleted 
            left join heropay_transactions_by_member hp on
                hp.member_uuid = m.uuid
            left join "dev"."employment_hero"."_v_employees_first_time_swag_app" as sa on
                lower(m.email) = sa.user_email
            join "dev"."employment_hero"."organisations" o on 
                m.organisation_id = o.id
                and o.id not in (select organisation_id from unlaunched_organisations)
            join "dev"."ebenefits"."_v_instapay_on_off_organisations" ioo on 
                ioo.organisation_id = m.organisation_id
                and ioo.instapay_enabled
            join "dev"."postgres_public"."menu_customisations" as mc on 
                mc.organisation_id = o.id
                and mc.instapay != 0
            left join  "dev"."employment_hero"."_v_active_employees_by_organisations" os on 
                m.organisation_id = os.organisation_id  
            left join  "dev"."one_platform"."industry" as oi on 
                regexp_replace( o.industry,'\\s','') = regexp_replace( oi.eh_industry,'\\s','')
        where 
            -- m.work_country = 'AU'  
            m.active
            and m.accepted
            and m.external_id is not null
            and m.synced_status != 1
            and m.sync_with_payroll
            and coalesce(m.termination_date, cast(json_extract_path_text(m.termination_info, 'termination_date') as date)) is null
    )

select * from instapay_members