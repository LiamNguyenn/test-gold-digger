{{ config(
    alias="eh_kp_combined_hours"    
    )
}}

with    
    business_organisation_overlap as (
        select distinct organisation_id, pr.business_id as kp_business_id
        from
            (
                select epa.organisation_id, external_id
                from  {{ref('employment_hero_v_last_connected_payroll')}} as epa
                join {{ source('postgres_public', 'payroll_infos') }} pi on payroll_info_id = pi.id
                where epa.type = 'KeypayAuth' and not pi._fivetran_deleted
            ) as o
        join
            {{ ref('keypay_t_pay_run_total_monthly_summary') }} pr on pr.business_id = o.external_id
    ),
    kp_hours as (
        select
            month,
            business_id,
            employee_id,
            residential_state,
            gender,
            employment_type,
            industry,
            total_employees,
            age,
            sum(monthly_hours) as monthly_hours
        from {{ ref("employment_index_v_median_hours_worked_kp") }}

        {% if is_incremental() %}       
        where month >= DATEADD('month', -12, current_date)
        {% endif %}

        group by 1, 2, 3, 4, 5, 6, 7, 8, 9
    ),
    combined_hours as (
        select
            month,
            member_id,
            organisation_id::bigint,
            gender,
            industry,
            residential_state,
            employment_type,
            total_employees,
            age,
            monthly_hours
        from {{ ref('employment_hero_au_employee_monthly_pay') }} p
        where
            organisation_id
            not in (select organisation_id from business_organisation_overlap)
            
            {% if is_incremental() %}    
            and month >= DATEADD('month', -12, current_date)
            {% endif %}
        union
        select
            month,
            employee_id as "member_id",
            business_id::bigint as organisation_id,
            gender,
            industry,
            residential_state,
            employment_type,
            total_employees,
            age,
            monthly_hours
        from kp_hours
    )
select *
from
    combined_hours
    -- select month,
    -- gender,
    -- count(distinct organisation_id), 
    -- count(organisation_id)
    -- from all_salary
    -- where month >= '2019-01-01'
    -- group by 1, 2
    
