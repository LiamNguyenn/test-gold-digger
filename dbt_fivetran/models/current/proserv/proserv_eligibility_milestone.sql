{{
    config(
        materialized='view',
        alias='eligibility_milestone'
    )
}}

select
    pt.project_id,
    t2.approval_status
from {{ source('asana', 'task') }} as t
inner join {{ source('asana', 'project_task') }} as pt on t.id = pt.task_id
inner join {{ source('workshop_public', 'asana_tasks') }} as t2 on t.id = t2.id
where t.name ~ 'Eligibility Milestone -' and not isnull(t._fivetran_deleted, 'f')
