select date, persona, monthly_users as swag_mau
from "dev"."mp"."swag_daumau_persona"
where swag_mau is not null
order by 1