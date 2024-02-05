



select
    1
from "dev"."exports"."exports_braze_users"

where not(strpos(last_name, chr(92) || chr(39)) = 0)

