select date, persona, monthly_users as swag_mau
from {{ ref("mp_swag_daumau_persona") }}
where swag_mau is not null
order by 1