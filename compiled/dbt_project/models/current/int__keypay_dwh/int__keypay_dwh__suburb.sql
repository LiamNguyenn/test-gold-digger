

select *
from "dev"."stg__keypay_dwh"."suburb"
where date_trunc('day', _transaction_date) = '2024-02-01'