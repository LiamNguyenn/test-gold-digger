{{ config(alias='au_pay_run_summary_s') }}

with
kp_business_industry as (
    select
        mapped_business.id,
        case
            when mapped_business.industry = 'Other' then 'Other'
            when mapped_business.industry != 'Other' and mapped_business.industry is not NULL then op_industry.consolidated_industry
        end as industry
    from
        (
            select
                kp_business.id,
                case
                    when kp_business.industry_id is NULL and kp_business.industry_name is not NULL
                        then 'Other'
                    when
                        kp_business.industry_id is NULL and kp_business.industry_name is NULL
                        and kp_zoom_info.primary_industry is not NULL and kp_zoom_info.primary_industry != ''
                        then kp_zoom_info.primary_industry
                    when kp_business.industry_id is not NULL then kp_industry.name
                end as industry
            from
                {{ ref('int__keypay_dwh__business') }} as kp_business
            left join (select
                id,
                name
            from {{ ref('int__keypay__industry') }}) as kp_industry
                on
                    kp_business.industry_id = kp_industry.id
            left join (select
                _id,
                primary_industry
            from {{ ref('int__keypay__zoom_info') }}) as kp_zoom_info on
                kp_business.id = kp_zoom_info._id
        ) as mapped_business
    left join {{ source('one_platform', 'industry') }} as op_industry on
        regexp_replace(mapped_business.industry, '\\s', '') = regexp_replace(op_industry.keypay_industry, '\\s', '')
        or regexp_replace(mapped_business.industry, '\\s', '') = regexp_replace(op_industry.zoom_info_primary_industry, '\\s', '')
        or regexp_replace(mapped_business.industry, '\\s', '') = regexp_replace(op_industry.eh_industry, '\\s', '')
),

billed_employees_by_business as (
    select
        billing_month,
        business_id,
        count(distinct employee_id) as billed_employees
    from {{ ref('mart__keypay__t_pay_run_total_monthly_summary') }}
    where
        not is_excluded_from_billing
        and business_id is not NULL
        and billing_month is not NULL
    group by 1, 2
),

--keypay.business_monthly_summary_062022 as bs 
billed_business as (
    select distinct coalesce(kp_invoice_line_item.business_id, kp_business.id) as business_id
    from {{ ref('int__keypay__invoice_line_item') }} as kp_invoice_line_item
    inner join {{ ref('int__keypay__invoice') }} as kp_invoice on kp_invoice_line_item.invoice_id = kp_invoice.id and split_part(kp_invoice._file, 'Shard', 2) = split_part(kp_invoice_line_item._file, 'Shard', 2)
    left join {{ ref('int__keypay_dwh__business') }} as kp_business on kp_invoice_line_item.abn = kp_business.abn and kp_invoice_line_item.business_id is NULL
),

keypay_employees as (
    select
        kp_employee.id,
        kp_employee.date_created,
        kp_employee.date_of_birth,
        kp_employee.residential_suburb_id,
        kp_employee.end_date,
        kp_employee.gender,
        kp_employee.business_id,
        kp_employee.start_date,
        kp_et.description as employment_type_description,
        kp_employee._file
    from {{ ref('int__keypay_dwh__employee') }} as kp_employee
    inner join {{ ref('int__keypay_dwh__business') }} as kp_business on kp_employee.business_id = kp_business.id
    left join {{ ref('int__keypay__white_label') }} as kp_whitelabel on kp_business.white_label_id = kp_whitelabel.id and split_part(kp_business._file, 'Shard', 2) = split_part(kp_whitelabel._file, 'Shard', 2)
    left join {{ ref('int__keypay__tax_file_declaration') }} as kp_tfd on kp_employee.tax_file_declaration_id = kp_tfd.id and kp_employee.id = kp_tfd.employee_id and split_part(kp_tfd._file, 'Shard', 2) = split_part(kp_employee._file, 'Shard', 2) -- AU
    left join {{ ref('int__keypay__employment_type') }} as kp_et on kp_tfd.employment_type_id = kp_et.id
    where
        (kp_employee.end_date is NULL or kp_employee.end_date >= kp_employee.date_created)
        and {{ legit_kp_name('surname') }}
        and {{ legit_kp_name('firstname') }}
),

employee_details as (
    select
        keypay_employees.id,
        keypay_employees.gender,
        keypay_employees.date_of_birth,
        keypay_employees.employment_type_description,
        keypay_employees.start_date,
        keypay_employees.end_date,
        kp_suburb.postcode as residential_postcode,
        kp_suburb.name     as residential_suburb,
        --, l.state as residential_state			
        case
            when kp_suburb.state ~* '(South Australia|SA)' then 'SA'
            when kp_suburb.state ~* '(Northern Territory|NT)' then 'NT'
            when kp_suburb.state ~* '(Victoria|VIC)' then 'VIC'
            when kp_suburb.state ~* '(New South|NSW)' then 'NSW'
            when kp_suburb.state ~* '(Queensland|QLD)' then 'QLD'
            when kp_suburb.state ~* '(Tasmania|TAS)' then 'TAS'
            when kp_suburb.state ~* '(Western Australia|WA)' then 'WA'
            when kp_suburb.state ~* '(Australian Capital Territory|ACT)' then 'ACT'
        end                as residential_state,
        kp_suburb.country  as residential_country
    --   distinct lower(surname) as sname
    --   ,lower(first_name) as fname
    --   count(*)
    from
        keypay_employees
    left join {{ ref('int__keypay_dwh__suburb') }} as kp_suburb
        on
            keypay_employees.residential_suburb_id = kp_suburb.id
--multiple matches of state for one postcode
--left join (select distinct postcode, state from csv.australian_postcodes_localities where sa_4_code_2016 is not null)l on s.postcode = l.postcode
)

select
    pr.employee_id,
    employee_details.residential_state,
    pr.business_id,
    kp_business_industry.industry,
    billed_employees.billed_employees                                                                                  as business_billed_employees,
    pr.invoice_id,
    pr.billing_month,
    pr.is_excluded_from_billing,
    pr.monthly_gross_earnings,
    pr.monthly_net_earnings,
    pr.total_hours,
    case when pr.total_hours = 0 then NULL else pr.monthly_gross_earnings / pr.total_hours::float end                  as hourly_rate,
    --, pr.payg_withholding_amount
    --, pr.help_amount
    --, pr.super_contribution      
    case when employee_details.gender = 'F' then 'Female' when employee_details.gender = 'M' then 'Male' end           as gender,
    datediff('year', employee_details.date_of_birth, pr.billing_month)                                                 as age,
    case
        when employee_details.employment_type_description = '' or employee_details.employment_type_description is NULL or employee_details.employment_type_description = 'NULL' then NULL
        when employee_details.employment_type_description = 'Full Time' then 'Full-time'
        when employee_details.employment_type_description = 'Part Time' then 'Part-time'
        else employee_details.employment_type_description
    end                                                                                                                as employment_type,
    employee_details.start_date,
    employee_details.end_date,
    (pr.monthly_gross_earnings - avg(pr.monthly_gross_earnings) over ()) / (stddev(pr.monthly_gross_earnings) over ()) as z_score_earnings,
    (pr.total_hours - avg(pr.total_hours) over ()) / (stddev(pr.total_hours) over ())                                  as z_score_hours,
    (hourly_rate - avg(hourly_rate) over ()) / (stddev(hourly_rate) over ())                                           as z_score_hourly_rate
from
    {{ ref('mart__keypay__t_pay_run_total_monthly_summary') }} as pr  -- noqa: AL06
inner join employee_details
    on
        pr.employee_id = employee_details.id
inner join {{ ref('int__keypay_dwh__business') }} as kp_business
    on
        pr.business_id = kp_business.id
left join {{ ref('int__keypay__white_label') }} as kp_whitelabel on kp_business.white_label_id = kp_whitelabel.id --AND SPLIT_PART(kp_business._file, 'Shard', 2) = SPLIT_PART(kp_whitelabel._file, 'Shard', 2)                                                   
inner join billed_business on kp_business.id = billed_business.business_id and billed_business.business_id is not NULL  --      and billed_business.month = pr.billing_month
left join billed_employees_by_business as billed_employees on pr.billing_month = billed_employees.billing_month and kp_business.id = billed_employees.business_id
left join kp_business_industry
    on
        kp_business.id = kp_business_industry.id
where
    pr.total_hours < 24 * 31
    and not pr.is_excluded_from_billing
    and (employee_details.employment_type_description != 'Superannuation Income Stream' or employee_details.employment_type_description is NULL)
    and (employee_details.employment_type_description != 'Labour Hire' or employee_details.employment_type_description is NULL)
    and pr.invoice_id is not NULL
    and (kp_whitelabel.region_id is NULL or kp_whitelabel.region_id = 1)  -- AU business      
    and (kp_whitelabel.reseller_id is NULL or kp_whitelabel.reseller_id not in (511, 829, 22, 708, 755, 790, 669)) -- exclude Test Partners
    and kp_business.abn != 11111111111
    and datediff('day', getdate(), kp_business.commence_billing_from::date) < 180
    --   and kp_business.name ilike '%test%' or name ilike '%demo%'     
    and pr.billing_month is not NULL
