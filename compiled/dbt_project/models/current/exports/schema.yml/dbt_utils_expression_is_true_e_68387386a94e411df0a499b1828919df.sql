



select
    1
from "dev"."exports"."exports_braze_users"

where not(phone_number_e164 not like '+610%')

