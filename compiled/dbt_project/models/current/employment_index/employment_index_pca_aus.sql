

with
    combined_pca as (
        select *
        from "dev"."employment_index"."eh_pay_category" p
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