{{ config(alias="pca_aus") }}

with
    -- business_organisation_overlap as (
    --     select distinct organisation_id, pr.business_id as kp_business_id
    --     from
    --         (
    --             select epa.organisation_id, external_id
    --             from {{ ref("employment_hero_v_last_connected_payroll") }} as epa
    --             join postgres_public.payroll_infos pi on payroll_info_id = pi.id
    --             where epa.type = 'KeypayAuth' and not pi._fivetran_deleted
    --         ) as o
    --     join
    --         keypay._t_pay_run_total_monthly_summary pr on pr.business_id = o.external_id
    -- ),

    combined_pca as (
        select *
        from {{ ref('employment_index_eh_pay_category') }} p
    ),
    overall_net_earnings as (
        select distinct
            category,
            month,
            median(net_earnings) over (
                partition by category, month
            ) as monthly_net_earnings
        from combined_pca
        where category is not null
        order by category, month
    )
select * from overall_net_earnings
