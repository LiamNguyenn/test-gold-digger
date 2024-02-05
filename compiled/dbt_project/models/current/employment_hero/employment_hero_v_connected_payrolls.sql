

     select * 
        from "dev"."postgres_public"."external_payroll_auths"
        where
        id in (
            select
                FIRST_VALUE(a.id) over(partition by a.payroll_info_id order by a.updated_at desc rows between unbounded preceding and unbounded following)
            from
                "dev"."postgres_public"."external_payroll_auths" a 
                join "dev"."postgres_public"."payroll_infos" i on a.payroll_info_id = i.id 
            where not a._fivetran_deleted
                and not i._fivetran_deleted
                and i.status = 1    --connected
        )