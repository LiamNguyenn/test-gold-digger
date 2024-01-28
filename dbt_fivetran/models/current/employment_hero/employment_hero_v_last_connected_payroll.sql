{{ config(materialized='view', alias='_v_last_connected_payroll') }}

     select * 
        from {{source('postgres_public', 'external_payroll_auths')}}
        where
        id in (
            select
                FIRST_VALUE(a.id) over(partition by a.organisation_id order by a.created_at desc rows between unbounded preceding and unbounded following)
            from
                {{source('postgres_public', 'external_payroll_auths')}} a 
                join {{source('postgres_public', 'payroll_infos')}} i on a.payroll_info_id = i.id 
            where not a._fivetran_deleted
                and not i._fivetran_deleted
                and i.status = 1    --connected
        )
        and not _fivetran_deleted                