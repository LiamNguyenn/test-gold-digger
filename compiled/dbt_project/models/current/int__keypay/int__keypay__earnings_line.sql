
select
    *
from "dev"."stg__keypay"."earnings_line"


  -- this filter will only be applied on an incremental run
  -- (uses > to include records whose timestamp occurred since the last run of this model)
  where _transaction_date > (select max(_transaction_date) from "dev"."int__keypay"."earnings_line")

