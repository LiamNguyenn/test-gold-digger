-- establish dates
with
    dates as (
        select distinct cast(dateadd('day', - generated_number::int, current_date) as date) as "date"
        from (

    

    with p as (
        select 0 as generated_number union all select 1
    ), unioned as (

    select

    
    p0.generated_number * power(2, 0)
     + 
    
    p1.generated_number * power(2, 1)
     + 
    
    p2.generated_number * power(2, 2)
     + 
    
    p3.generated_number * power(2, 3)
     + 
    
    p4.generated_number * power(2, 4)
     + 
    
    p5.generated_number * power(2, 5)
     + 
    
    p6.generated_number * power(2, 6)
     + 
    
    p7.generated_number * power(2, 7)
     + 
    
    p8.generated_number * power(2, 8)
     + 
    
    p9.generated_number * power(2, 9)
    
    
    + 1
    as generated_number

    from

    
    p as p0
     cross join 
    
    p as p1
     cross join 
    
    p as p2
     cross join 
    
    p as p3
     cross join 
    
    p as p4
     cross join 
    
    p as p5
     cross join 
    
    p as p6
     cross join 
    
    p as p7
     cross join 
    
    p as p8
     cross join 
    
    p as p9
    
    

    )

    select *
    from unioned
    where generated_number <= 761
    order by generated_number

)
    ),
    ces as (
        select distinct
            r.response_date::date as date,
            (
                case
                    when r.country in ('Australia', 'Singapore', 'United Kingdom', 'Malaysia', 'New Zealand')
                    then r.country
                    else 'untracked'
                end
            ) as country,
            count(case when lower(mo.product_family) like '%talent%' then average_score end) as talent_total_count,
            sum(case when lower(mo.product_family) like '%talent%' then average_score end) as talent_total_score,
            count(case when lower(mo.product_family) like '%eben%' then average_score end) as eben_total_count,
            sum(case when lower(mo.product_family) like '%eben%' then average_score end) as eben_total_score,
            count(
                case
                    when lower(mo.product_family) not like '%payroll%' or lower(mo.product_family) = 'pre-payroll'
                    then average_score
                end
            ) as hr_total_count,
            sum(
                case
                    when lower(mo.product_family) not like '%payroll%' or lower(mo.product_family) = 'pre-payroll'
                    then average_score
                end
            ) as hr_total_score,
            count(
                case
                    when lower(mo.product_family) like '%payroll%' and lower(mo.product_family) != 'pre-payroll'
                    then average_score
                end
            ) as payroll_total_count,
            sum(
                case
                    when lower(mo.product_family) like '%payroll%' and lower(mo.product_family) != 'pre-payroll'
                    then average_score
                end
            ) as payroll_total_score
        from "dev"."ces"."responses" r
        left join "dev"."eh_product"."module_ownership" mo on lower(mo.event_module) = lower(r.module)
        group by 1, 2
    ),
    product_mapping as (
        select
            po.product_family,
            wo.product_line,
            po.workstream,
            po.hr_page as product,
            po.product_owner,
            case when po.payroll_integration = 'no' then 0 else 1 end as payroll_integration,
            case
                -- Billing was moved from General Settings sidebar
                when po.sidebar = 'Billings'
                then 'hr_endpoint_general_settings__billing'
                -- To cater for the legacy tagging on Recruitment sidebar 
                when po.sidebar = 'Recruitment' and lower(po.sub_category) = 'ats'
                then 'hr_endpoint_recruitment__ats_'
                when po.sidebar = 'Recruitment' and lower(po.sub_category) = 'job posting'
                then 'hr_endpoint_recruitment__ats___post_to_job_board'
                when po.sidebar = 'Recruitment' and lower(po.sub_category) = 'manage job board'
                then 'hr_endpoint_recruitment__ats___manage_job_board_'
                when po.sidebar = 'Recruitment'
                then 'hr_endpoint_recruitment__ats___' + lower(replace(po.sub_category, ' ', '_'))
                else
                    'hr_endpoint_'
                    + lower(regexp_replace(replace(po.hr_page, '> ', '>'), '[^a-zA-Z0-9_&()>]+|[&()>]', '_'))
            end as legacy_tag,
            'employment_hero_hr_' + lower(regexp_replace(po.sidebar, '[^a-zA-Z0-9_&()]+|[&()]', '_')) as sidebar_tag,
            lower(
                regexp_replace((po.sidebar + '_' + po.sub_category), '[^a-zA-Z0-9_&()]+|[&()]', '_')
            ) as sub_category_tag,
            lower(
                regexp_replace((po.sub_category + '_' + po.sub_sub_category), '[^a-zA-Z0-9_&()]+|[&()]', '_')
            ) as sub_sub_category_tag,
            coalesce(sub_sub_category_tag, sub_category_tag, sidebar_tag) as feature_tag
        from "dev"."eh_product"."product_ownership" as po
        left join "dev"."eh_product"."workstream_ownership" as wo on po.workstream = wo.workstream
        order by po.hr_page
    ),
    ticket as (
        select distinct
            t.created_at::date as date,
            (
                case
                    when lower(t.custom_country) = 'au'
                    then 'Australia'
                    when lower(t.custom_country) = 'uk'
                    then 'United Kingdom'
                    when lower(t.custom_country) = 'nz'
                    then 'New Zealand'
                    when lower(t.custom_country) = 'my'
                    then 'Malaysia'
                    when lower(t.custom_country) = 'sg'
                    then 'Singapore'
                    else 'untracked'
                end
            ) as country,
            count(distinct(case when lower(g.name) like '%hr%' then t.id end)) as hr_tickets,
            count(distinct(case when lower(g.name) like '%payroll%' then t.id end)) as payroll_tickets,
            count(
                distinct(
                    case
                        when lower(po.product_family) like '%eben%' or lower(po.product_line) like '%eben%' then t.id
                    end
                )
            ) as eben_tickets,
            count(distinct(case when lower(po.product_family) like '%talent%' then t.id end)) as talent_tickets
        from "dev"."zendesk"."ticket" t
        left join "dev"."zendesk"."ticket_tag" tt on t.id = tt.ticket_id
        left join product_mapping po on tt."tag" = po.feature_tag or tt."tag" = po.legacy_tag
        left join "dev"."zendesk"."group" g on t.group_id = g.id
        group by 1, 2
    )
select
    d.date,
    (
        case
            when t.country is not null then t.country when ces.country is not null then ces.country else 'untracked'
        end
    ) as country,
    ces.talent_total_count,
    ces.talent_total_score,
    ces.eben_total_count,
    ces.eben_total_score,
    ces.hr_total_count,
    ces.hr_total_score,
    ces.payroll_total_count,
    ces.payroll_total_score,
    t.hr_tickets,
    t.payroll_tickets,
    t.eben_tickets,
    t.talent_tickets
from dates d
left join ticket t on d.date = t.date
left join ces on ces.date = d.date and ces.country = t.country