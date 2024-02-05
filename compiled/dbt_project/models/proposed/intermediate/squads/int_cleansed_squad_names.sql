with squads_raw as (
    select
        squad_owner,
        case
            when squad_owner like 'squad%'
                then split_part(squad_owner, 'squad-', 2)
            else squad_owner
        end as squad
    from "dev"."staging"."stg_eh_infra_stat_service_raw__daily_report_sentry_issues"
    where squad_owner is not NULL
),

cleansed_squad_names as (
    select
        squad_owner,
        case
            when squad like 'autobot'
                then 'autobots'
            when squad like 'night%'
                then 'night''s watch'
            else squad
        end as squad
    from (
        select
            squad_owner,
            replace(squad, '-', ' ') as squad
        from squads_raw
    )
)

select distinct
    squad_owner,
    case
        when squad like '%''%' then replace(initcap(replace(squad, '''', 'asdf')), 'asdf', '''') --this is to accommodate quotes in squad_owner field, the quotes are first replaced with a random string which is unlikely to show up in an actual squad name and replaced back after applying the initcap function
        else initcap(squad)
    end as squad
from cleansed_squad_names