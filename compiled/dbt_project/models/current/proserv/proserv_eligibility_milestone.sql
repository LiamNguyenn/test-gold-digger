

select
    pt.project_id,
    t2.approval_status
from "dev"."asana"."task" as t
inner join "dev"."asana"."project_task" as pt on t.id = pt.task_id
inner join "dev"."workshop_public"."asana_tasks" as t2 on t.id = t2.id
where t.name ~ 'Eligibility Milestone -' and not isnull(t._fivetran_deleted, 'f')