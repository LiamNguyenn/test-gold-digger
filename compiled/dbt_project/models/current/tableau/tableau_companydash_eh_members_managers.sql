with recursive
    eh_members as (
        select distinct
            uuid,
            id,
            first_name,
            last_name,
            active,
            work_country,
            gender,
            cast(start_date as date) start_date,
            cast(termination_date as date) termination_date,
            (
                case
                    when active = true
                    then 'N/A'
                    when
                        termination_info like '%termination_type":"Transfer"%'
                        or termination_info like '%termination_code":"T"%'
                    then 'Transfer'
                    when
                        lower(termination_info) like '%volun%'
                        or termination_info like '%code":"V"%'
                        or lower(termination_info) like '%resign%'
                        or lower(termination_info) like '%regret%'
                    then 'Voluntary'
                    else 'Involuntary'
                end
            ) as termination_type,
            termination_info,
            (
                case
                    when
                        dateadd('month', cast(probation_length as integer), cast(start_date as date))
                        <= cast(termination_date as date)
                    then false
                    else true
                end
            ) as if_pass_probation
        from "dev"."postgres_public"."members"
        where organisation_id = 8701 and _fivetran_deleted = 'f' and is_shadow_data = false and system_user = false
    ),
    direct_manager as (
        select
            (m.first_name || ' ' || m.last_name) as employee_name,
            m.id member_id,
            mm.manager_id,
            (m2.first_name || ' ' || m2.last_name) as manager_name,
            mm.updated_at
        from "dev"."postgres_public"."members" m
        left join
            "dev"."postgres_public"."member_managers" mm
            on mm.member_id = m.id
            and mm.level = 1
            and mm.order = 0
            and mm._fivetran_deleted = false
        left join
            "dev"."postgres_public"."members" m2
            on mm.manager_id = m2.id
            and m2.organisation_id = 8701
            and m2._fivetran_deleted = false
            and m2.system_user = false
        where m.organisation_id = 8701 and m._fivetran_deleted = false and m.system_user = false
    ),
    -- recursive here
    manager(employee_name, member_id, manager_id, manager_name, path) as (
        select employee_name, member_id, manager_id, manager_name, employee_name as path
        from direct_manager
        where manager_id is null and employee_name = 'Benjamin Thompson'
        union all
        select dm.employee_name, dm.member_id, dm.manager_id, dm.manager_name, (m.path || '-' || dm.manager_name)
        from direct_manager dm, manager m
        where dm.manager_id = m.member_id
    )

-- join back to the members table
select distinct
    m.id member_id,
    m.uuid member_uuid,
    (m.first_name || ' ' || m.last_name) as employee_name,
    m.active if_active_employee,
    (
        case
            when m.active = true
            then datediff('day', m.start_date, current_date)
            else datediff('day', m.start_date, m.termination_date)
        end
    ) as tenure,
    m.start_date,
    m.termination_date,
    m.gender,
    m.work_country,
    m.if_pass_probation,
    m.termination_info,
    m.termination_type,
    m2.manager_name,
    substring(m2.path, 19, len(m2.path) - 17) as path
from
    -- clean up from members table
    (
        select distinct
            id,
            uuid,
            first_name,
            last_name,
            active,
            work_country,
            gender,
            start_date,
            termination_date,
            termination_type,
            termination_info,
            if_pass_probation
        from eh_members
    )
    m
left join manager m2 on m2.member_id = m.id